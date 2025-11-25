import { DockgeServer } from "./dockge-server";
import { DockgeSocket } from "./util-server";
import { log } from "./log";
import * as childProcess from "child_process";

/**
 * Raw JSON output from `docker compose stats --format json`
 */
export interface DockerStatsRaw {
    BlockIO: string;
    CPUPerc: string;
    Container: string;
    ID: string;
    MemPerc: string;
    MemUsage: string;
    Name: string;
    NetIO: string;
    PIDs: string;
}

/**
 * Parsed container stats for frontend
 */
export interface ContainerStats {
    id: string;
    name: string;
    service: string;
    cpuPercent: string;
    memoryUsage: string;
}

/**
 * Stats Runner for docker compose stats
 * Unlike Terminal, this parses JSON data and emits structured data to the frontend
 */
export class StatsRunner {
    protected static statsMap: Map<string, StatsRunner> = new Map();

    protected server: DockgeServer;
    protected name: string;
    protected cwd: string;
    protected process?: childProcess.ChildProcess;
    protected socketList: Record<string, DockgeSocket> = {};
    protected keepAliveInterval?: NodeJS.Timeout;
    protected kickDisconnectedClientsInterval?: NodeJS.Timeout;
    protected buffer: string = "";

    constructor(server: DockgeServer, name: string, cwd: string) {
        this.server = server;
        this.name = name;
        this.cwd = cwd;
        StatsRunner.statsMap.set(name, this);
    }

    public start() {
        if (this.process) {
            return;
        }

        this.kickDisconnectedClientsInterval = setInterval(() => {
            for (const socketID in this.socketList) {
                const socket = this.socketList[socketID];
                if (!socket.connected) {
                    log.debug("StatsRunner", "Kicking disconnected client " + socket.id + " from stats " + this.name);
                    this.leave(socket);
                }
            }
        }, 60 * 1000);

        this.keepAliveInterval = setInterval(() => {
            const numClients = Object.keys(this.socketList).length;
            if (numClients === 0) {
                log.debug("StatsRunner", "Stats " + this.name + " has no client, closing...");
                this.stop();
            }
        }, 60 * 1000);

        try {
            this.process = childProcess.spawn("docker", [
                "compose",
                "stats",
                "--no-trunc",
                "--format",
                "json"
            ], {
                cwd: this.cwd,
            });

            this.process.stdout?.on("data", (data: Buffer) => {
                this.buffer += data.toString();
                this.processBuffer();
            });

            this.process.stderr?.on("data", (data: Buffer) => {
                log.debug("StatsRunner", "stderr: " + data.toString());
            });

            this.process.on("exit", (code) => {
                log.debug("StatsRunner", "Stats " + this.name + " exited with code " + code);
                this.cleanup();
            });

            this.process.on("error", (err) => {
                log.error("StatsRunner", "Failed to start stats: " + err.message);
                this.cleanup();
            });

        } catch (error) {
            if (error instanceof Error) {
                log.error("StatsRunner", "Failed to start stats: " + error.message);
            }
            this.cleanup();
        }
    }

    /**
     * Process buffer and extract complete JSON objects
     * Use Map to deduplicate by container name
     */
    protected processBuffer() {
        // Use Map to deduplicate by container name
        const statsMap: Map<string, ContainerStats> = new Map();
        let depth = 0;
        let startIndex = 0;
        let lastCompleteEnd = -1;

        for (let i = 0; i < this.buffer.length; i++) {
            const char = this.buffer[i];

            if (char === "{") {
                if (depth === 0) {
                    startIndex = i;
                }
                depth++;
            } else if (char === "}") {
                depth--;
                if (depth === 0) {
                    const jsonStr = this.buffer.substring(startIndex, i + 1);
                    const stats = this.parseStatsJson(jsonStr);
                    if (stats) {
                        // Use container name as key to deduplicate
                        statsMap.set(stats.id, stats);
                    }
                    lastCompleteEnd = i;
                }
            }
        }

        // Remove processed part from buffer
        if (lastCompleteEnd >= 0) {
            this.buffer = this.buffer.substring(lastCompleteEnd + 1);
        }

        // Emit batch if we have any stats
        if (statsMap.size > 0) {
            const statsList = Array.from(statsMap.values());
            // this.emitStats(statsList);
            for (const socketID in this.socketList) {
                const socket = this.socketList[socketID];
                socket.emitAgent("composeStats", this.name, statsList);
            }
        }
    }

    /**
     * Parse a single JSON string to ContainerStats
     */
    protected parseStatsJson(jsonStr: string): ContainerStats | null {
        try {
            const rawStats = JSON.parse(jsonStr) as DockerStatsRaw;
            return this.parseStats(rawStats);
        } catch (e) {
            if (e instanceof Error) {
                log.debug("StatsRunner", "Failed to parse stats JSON: " + e.message);
            }
            return null;
        }
    }

    /**
     * Emit stats list to all connected sockets
     */
    protected emitStats(statsList: ContainerStats[]) {
        for (const socketID in this.socketList) {
            const socket = this.socketList[socketID];
            socket.emitAgent("composeStats", this.name, statsList);
        }
    }

    /**
     * Parse raw docker stats JSON to structured data
     */
    protected parseStats(raw: DockerStatsRaw): ContainerStats {
        return {
            id: raw.Container || "",
            name: raw.Name || raw.Container || "",
            service: this.extractServiceName(raw.Name || ""),
            cpuPercent: raw.CPUPerc || "0.00%",
            memoryUsage: `${this.parseMemoryUsage(raw.MemUsage)} / ${this.parseMemoryLimit(raw.MemUsage)}`,
        };
    }

    /**
     * Extract service name from container name
     * Container name format is usually: stackname-servicename-1
     */
    protected extractServiceName(containerName: string): string {
        const parts = containerName.split("-");
        if (parts.length >= 2) {
            parts.pop();
            parts.shift();
            return parts.join("-");
        }
        return containerName;
    }

    protected parseMemoryUsage(memUsage: string): string {
        if (!memUsage) {
            return "0B";
        }
        const parts = memUsage.split(" / ");
        return parts[0] || "0B";
    }

    protected parseMemoryLimit(memUsage: string): string {
        if (!memUsage) {
            return "0B";
        }
        const parts = memUsage.split(" / ");
        return parts[1] || "0B";
    }

    public join(socket: DockgeSocket) {
        this.socketList[socket.id] = socket;
    }

    public leave(socket: DockgeSocket) {
        delete this.socketList[socket.id];
    }

    public stop() {
        if (this.process) {
            this.process.kill("SIGTERM");
        }
        this.cleanup();
    }

    protected cleanup() {
        clearInterval(this.keepAliveInterval);
        clearInterval(this.kickDisconnectedClientsInterval);
        this.socketList = {};
        this.buffer = "";
        StatsRunner.statsMap.delete(this.name);
        this.process = undefined;
    }

    public static getStatsRunner(name: string): StatsRunner | undefined {
        return StatsRunner.statsMap.get(name);
    }

    public static getOrCreateStatsRunner(server: DockgeServer, name: string, cwd: string): StatsRunner {
        let runner = StatsRunner.getStatsRunner(name);
        if (!runner) {
            runner = new StatsRunner(server, name, cwd);
        }
        return runner;
    }
}
