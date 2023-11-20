import Keyv from 'keyv';

export interface CacheSetOptions {
  // in milliseconds
  ttl?: number;
}

// extends if needed
export interface Cache {
  // standard operation
  get(key: string): Promise<any>;
  set(key: string, value: any, opts?: CacheSetOptions): Promise<boolean>;
  setnx(key: string, value: any, opts?: CacheSetOptions): Promise<boolean>;
  increase(key: string, count?: number): Promise<number>;
  decrease(key: string, count?: number): Promise<number>;
  delete(key: string): Promise<boolean>;
  has(key: string): Promise<boolean>;
  ttl(key: string): Promise<number>;
  expire(key: string, ttl: number): Promise<boolean>;

  // list operations
  pushBack(key: string, ...values: any[]): Promise<number>;
  pushFront(key: string, ...values: any[]): Promise<number>;
  len(key: string): Promise<number>;
  list(key: string, start: number, count?: number): Promise<any[]>;
  popFront(key: string, count?: number): Promise<any[]>;
  popBack(key: string, count?: number): Promise<any[]>;

  // map operations
  mapSet(
    map: string,
    key: string,
    value: any,
    opts: CacheSetOptions
  ): Promise<boolean>;
  mapIncrease(map: string, key: string, count?: number): Promise<number>;
  mapDecrease(map: string, key: string, count?: number): Promise<number>;
  mapGet(map: string, key: string): Promise<any>;
  mapDelete(map: string, key: string): Promise<boolean>;
  mapRandomKey(map: string): Promise<string | undefined>;
  mapLen(map: string): Promise<number>;
}

export class LocalCache implements Cache {
  private readonly kv: Keyv<any>;

  constructor() {
    this.kv = new Keyv();
  }

  // standard operation
  async get(key: string): Promise<any> {
    return this.kv.get(key).catch(() => undefined);
  }

  async set(
    key: string,
    value: any,
    opts: CacheSetOptions = {}
  ): Promise<boolean> {
    return this.kv
      .set(key, value, opts.ttl)
      .then(() => true)
      .catch(() => false);
  }

  async increase(key: string, count: number = 1): Promise<number> {
    const prev = (await this.get(key)) ?? 0;
    if (typeof prev !== 'number') {
      throw new Error(
        `Expect a Number keyed by ${key}, but found ${typeof prev}`
      );
    }

    const curr = prev + count;
    return (await this.set(key, curr)) ? curr : prev;
  }

  async decrease(key: string, count: number = 1): Promise<number> {
    return this.increase(key, -count);
  }

  async setnx(
    key: string,
    value: any,
    opts?: CacheSetOptions | undefined
  ): Promise<boolean> {
    if (!(await this.has(key))) {
      return this.set(key, value, opts);
    }
    return false;
  }

  async delete(key: string): Promise<boolean> {
    return this.kv.delete(key).catch(() => false);
  }

  async has(key: string): Promise<boolean> {
    return this.kv.has(key).catch(() => false);
  }

  async ttl(key: string): Promise<number> {
    return this.kv
      .get(key, { raw: true })
      .then(raw => raw?.expires ?? Infinity)
      .catch(() => 0);
  }

  async expire(key: string, ttl: number): Promise<boolean> {
    const value = await this.kv.get(key);
    return this.set(key, value, { ttl });
  }

  // list operations
  private async getArray(key: string) {
    const raw = await this.kv.get(key, { raw: true });
    if (raw && !Array.isArray(raw.value)) {
      throw new Error(
        `Expect an Array keyed by ${key}, but found ${raw.value}`
      );
    }

    return raw;
  }

  private async setArray(
    key: string,
    value: any[],
    opts: CacheSetOptions = {}
  ) {
    return this.set(key, value, opts).then(() => value.length);
  }

  async pushBack(key: string, ...values: any[]): Promise<number> {
    let list: any[] = [];
    let ttl: number | undefined = undefined;
    const raw = await this.getArray(key);
    if (raw) {
      list = raw.value;
      if (raw.expires) {
        ttl = raw.expires - Date.now();
      }
    }

    list = list.concat(values);
    return this.setArray(key, list, { ttl });
  }

  async pushFront(key: string, ...values: any[]): Promise<number> {
    let list: any[] = [];
    let ttl: number | undefined = undefined;
    const raw = await this.getArray(key);
    if (raw) {
      list = raw.value;
      if (raw.expires) {
        ttl = raw.expires - Date.now();
      }
    }

    list = values.concat(list);
    return this.setArray(key, list, { ttl });
  }

  async len(key: string): Promise<number> {
    return this.getArray(key).then(v => v?.value.length ?? 0);
  }

  async list(key: string, start: number, count: number = 1): Promise<any[]> {
    const raw = await this.getArray(key);
    if (raw?.value) {
      return raw.value.slice(start, start + count);
    } else {
      return [];
    }
  }

  private async trim(
    key: string,
    start: number,
    length: number
  ): Promise<any[]> {
    const list = await this.getArray(key);
    if (list) {
      const result = list.value.splice(start, start + length);
      await this.set(key, list);
      return result;
    }

    return [];
  }

  async popFront(key: string, count: number = 1): Promise<any[]> {
    return this.trim(key, 0, count);
  }

  async popBack(key: string, count: number = 1): Promise<any[]> {
    return this.trim(key, -count, count);
  }

  // map operations
  private async getMap(map: string) {
    const raw = await this.kv.get(map, { raw: true });

    if (raw) {
      if (typeof raw.value !== 'object') {
        throw new Error(
          `Expect an Object keyed by ${map}, but found ${typeof raw}`
        );
      }

      if (Array.isArray(raw.value)) {
        throw new Error(`Expect an Object keyed by ${map}, but found an Array`);
      }
    }

    return raw;
  }

  private async setMap(
    map: string,
    value: Record<string, any>,
    opts: CacheSetOptions = {}
  ) {
    return this.kv.set(map, value, opts.ttl).then(() => false);
  }

  async mapGet(map: string, key: string): Promise<any> {
    const raw = await this.getMap(map);
    if (raw?.value) {
      return raw.value[key];
    }

    return undefined;
  }

  async mapSet(map: string, key: string, value: any): Promise<boolean> {
    const raw = await this.getMap(map);
    const data = raw?.value ?? {};

    data[key] = value;

    return this.setMap(map, data, {
      ttl: raw?.expires ? raw.expires - Date.now() : undefined,
    });
  }

  async mapDelete(map: string, key: string): Promise<boolean> {
    const raw = await this.getMap(map);

    if (raw?.value) {
      delete raw.value[key];
      return this.setMap(map, raw.value, {
        ttl: raw.expires ? raw.expires - Date.now() : undefined,
      });
    }

    return false;
  }

  async mapIncrease(
    map: string,
    key: string,
    count: number = 1
  ): Promise<number> {
    const prev = this.mapGet(map, key);

    if (typeof prev !== 'number') {
      throw new Error(
        `Expect a Number keyed by ${key}, but found ${typeof prev}`
      );
    }

    const curr = prev + count;

    return (await this.mapSet(map, key, curr)) ? curr : prev;
  }

  async mapDecrease(
    map: string,
    key: string,
    count: number = 1
  ): Promise<number> {
    return this.mapIncrease(map, key, -count);
  }

  async mapRandomKey(map: string): Promise<string | undefined> {
    const raw = await this.getMap(map);
    if (raw) {
      const keys = Object.keys(raw.value);
      return keys[Math.floor(Math.random() * keys.length)];
    }

    return undefined;
  }

  async mapLen(map: string): Promise<number> {
    const raw = await this.getMap(map);
    return raw ? Object.keys(raw.value).length : 0;
  }
}
