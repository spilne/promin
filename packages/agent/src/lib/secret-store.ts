import { createCipheriv, createDecipheriv, randomBytes, scryptSync } from "node:crypto";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";

export interface SecretStore {
  get(key: string): Promise<string | undefined>;
  set(key: string, value: string): Promise<void>;
  has(key: string): Promise<boolean>;
  delete(key: string): Promise<void>;
}

// ---- InMemorySecretStore ----

export class InMemorySecretStore implements SecretStore {
  private readonly secrets = new Map<string, string>();

  async get(key: string): Promise<string | undefined> {
    return this.secrets.get(key);
  }
  async set(key: string, value: string): Promise<void> {
    this.secrets.set(key, value);
  }
  async has(key: string): Promise<boolean> {
    return this.secrets.has(key);
  }
  async delete(key: string): Promise<void> {
    this.secrets.delete(key);
  }
}

// ---- EnvSecretStore ----

/**
 * Read-only store backed by process.env. Useful for injecting secrets that
 * are already present in the environment without any extra config.
 */
export class EnvSecretStore implements SecretStore {
  async get(key: string): Promise<string | undefined> {
    return process.env[key];
  }
  async set(_key: string, _value: string): Promise<void> {
    throw new Error("EnvSecretStore is read-only");
  }
  async has(key: string): Promise<boolean> {
    return key in process.env;
  }
  async delete(_key: string): Promise<void> {
    throw new Error("EnvSecretStore is read-only");
  }
}

// ---- FileSecretStore ----

interface EncryptedEntry {
  iv: string;
  tag: string;
  data: string;
}

/**
 * Persistent store that encrypts secrets with AES-256-GCM before writing to disk.
 * The passphrase is stretched via scrypt — never stored alongside the data.
 */
export class FileSecretStore implements SecretStore {
  private readonly key: Buffer;
  private cache: Record<string, string> | null = null;

  constructor(private readonly config: { path: string; passphrase: string }) {
    this.key = scryptSync(config.passphrase, "promin-agent-secrets-v1", 32);
  }

  private async load(): Promise<Record<string, string>> {
    if (this.cache) return this.cache;

    let raw: string;
    try {
      raw = await readFile(this.config.path, "utf8");
    } catch {
      // File not found — treat as empty store
      this.cache = {};
      return {};
    }

    // Parse and decrypt — errors here (wrong passphrase, corrupt file) propagate
    const entries = JSON.parse(raw) as Record<string, EncryptedEntry>;
    const result: Record<string, string> = {};
    for (const [k, v] of Object.entries(entries)) {
      const iv = Buffer.from(v.iv, "hex");
      const tag = Buffer.from(v.tag, "hex");
      const decipher = createDecipheriv("aes-256-gcm", this.key, iv);
      decipher.setAuthTag(tag);
      result[k] = decipher.update(v.data, "hex", "utf8") + decipher.final("utf8");
    }
    this.cache = result;
    return result;
  }

  private async save(secrets: Record<string, string>): Promise<void> {
    const entries: Record<string, EncryptedEntry> = {};
    for (const [k, v] of Object.entries(secrets)) {
      const iv = randomBytes(12);
      const cipher = createCipheriv("aes-256-gcm", this.key, iv);
      const data = cipher.update(v, "utf8", "hex") + cipher.final("hex");
      entries[k] = {
        iv: iv.toString("hex"),
        tag: cipher.getAuthTag().toString("hex"),
        data,
      };
    }
    await mkdir(dirname(this.config.path), { recursive: true });
    await writeFile(this.config.path, JSON.stringify(entries, null, 2), "utf8");
    this.cache = secrets;
  }

  async get(key: string): Promise<string | undefined> {
    return (await this.load())[key];
  }

  async set(key: string, value: string): Promise<void> {
    const secrets = await this.load();
    await this.save({ ...secrets, [key]: value });
  }

  async has(key: string): Promise<boolean> {
    return key in (await this.load());
  }

  async delete(key: string): Promise<void> {
    const secrets = await this.load();
    const { [key]: _, ...rest } = secrets;
    await this.save(rest);
  }
}

// ---- CompositeSecretStore ----

/**
 * Chains multiple stores. Reads from the first store that has the key.
 * Writes always go to the first (primary) store.
 *
 * Useful pattern: EnvSecretStore (read-only env) + FileSecretStore (persistent user-provided).
 *   const store = new CompositeSecretStore([envStore, fileStore]);
 */
export class CompositeSecretStore implements SecretStore {
  constructor(private readonly stores: SecretStore[]) {}

  async get(key: string): Promise<string | undefined> {
    for (const s of this.stores) {
      const v = await s.get(key);
      if (v !== undefined) return v;
    }
    return undefined;
  }

  async set(key: string, value: string): Promise<void> {
    for (const s of this.stores) {
      try {
        await s.set(key, value);
        return;
      } catch {
        // read-only store — try next
      }
    }
    throw new Error("CompositeSecretStore: no writable store available");
  }

  async has(key: string): Promise<boolean> {
    for (const s of this.stores) {
      if (await s.has(key)) return true;
    }
    return false;
  }

  async delete(key: string): Promise<void> {
    for (const s of this.stores) {
      try {
        await s.delete(key);
        return;
      } catch {
        // read-only store — try next
      }
    }
    throw new Error("CompositeSecretStore: no writable store available");
  }
}
