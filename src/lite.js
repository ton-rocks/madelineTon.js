import Objects from "./TL/objects";
import Parser from "./TL/parser";
import Stream from "./TL/stream";
import ADNLConnection from "./adnl-connection";
import {
    fastRandomInt
} from "./crypto-sync/random";
import deepEqual from 'deep-equal'
import {
    bufferViewEqual,
    atobInt8
} from "./tools";
import CryptoAsync from "./crypto";
import {
    crc16
} from "./crypto-sync/crypto";
import schemeTON from './config/ton_api.json'
import schemeLite from './config/lite_api.json'
import liteConfig from './config/ton-lite-client-test1.config.json'
import BitStream from "./boc/bitstream";

class Lite {
    min_ls_version = 0x101
    min_ls_capabilities = 1
    server = {
        ok: false
    }

    knownBlockIds = []
    printedBlockIds = 0

    connections = []
    constructor(settings) {
        settings = {
            schemes: {
                1: schemeTON,
                2: schemeLite
            },
            config: liteConfig,
            wssProxies: {
                861606190: 'wss://ton-ws.madelineproto.xyz/testnetDebug',
                1137658550: 'wss://ton-ws.madelineproto.xyz/testnet'
            },
            maxConnections: 3,
           ...settings
        }
        let config = settings.config
        delete settings.config
        this.settings = settings

        this.TLObjects = new Objects(settings['schemes'])
        this.TLParser = new Parser(
            this.TLObjects, {
                typeConversion: {
                    'liteServer.AccountId': data => this.unpackAccountId(data)
                }
            }
        )

        let stream = new Stream

        // This is also a TL serializer test
        config['_'] = 'liteclient.config.global'
        config['validator']['init_block'] = config['validator']['init_block'] || config['validator']['zero_state']
        config = this.TLParser.serialize(stream, config)
        config.pos = 0
        this.config = this.TLParser.deserialize(config)

        this.crypto = new CryptoAsync(this.TLParser)
    }
    /**
     * Unpack account ID to a liteServer.accountId object
     * @param {string} data Account ID
     */
    unpackAccountId(data) {
        data = atobInt8(data.replace(/\-/g, '+').replace(/_/g, '/'))
        const crc = crc16(data.subarray(0, 34))
        if (!bufferViewEqual(crc, data.subarray(34))) {
            throw new Error('Invalid account ID provided, crc16 invalid!')
        }
        let result = {
            _: 'liteServer.accountId',
            flags: data[0]
        }
        if ((result.flags & 0x3f) != 0x11) {
            throw new Error('Invalid account ID, wrong flags')
        }
        result.testnet = result.flags & 0x80
        result.bounceable = !(result.flags & 0x40)
        result.workchain = data[1] > 0x7F ? -(0x100 - data[1]) : data[1]
        result.id = new Uint32Array(data.slice(2, 34).buffer)

        return result
    }
    /**
     * Deserialize bag of cells
     * @param {Uint8Array} cell Serialized cell data
     */
    slice(cell) {
        if (!cell.byteLength) {
            return {}
        }
        // This should be all done automatically by the TL parser
        console.log(cell)

        let stream = new BitStream(cell.buffer)
        const crc = stream.readBits(32)
        if (crc !== 3052313714) {
            throw new Error(`Invalid BOC constructor ${crc}`)
        }
        let result = {
            _: 'serialized_boc',
            has_idx: stream.readBits(1),
            has_crc32c: stream.readBits(1),
            has_cache_bits: stream.readBits(1),
            flags: stream.readBits(2),
            size: stream.readBits(3),
            off_bytes: stream.readBits(8),
        }
        const size = result.size * 8
        result.cell_count = stream.readBits(size)
        result.roots = stream.readBits(size)
        result.absent = stream.readBits(size)
        result.tot_cells_size = stream.readBits(result.off_bytes * 8)
        result.root_list = stream.readBits(result.roots * size)

        if (result.has_idx) {
            result.index = stream.readBits(result.cell_count * result.off_bytes * 8)
        }
        result.cell_data = stream.readBits(result.tot_cells_size * 8, false)
        if (result.has_crc32c) {
            result.crc32c = stream.readBits(32)
        }

        stream = new BitStream(result.cell_data.buffer)
        result.cellsRaw = []
        result.cells = []
        for (let x = 0; x < result.cell_count; x++) {
            // Will optimize later
            result.cellsRaw.push(this.deserializeCell(stream, size))
            result.cells.push(new BitStream(result.cellsRaw[x].data.buffer))
        }

        for (let x = 0; x < result.cell_count; x++) {
            for (let ref in result.cellsRaw[x].refs) {
                result.cells[x].pushRef(result.cells[ref])
            }
        }
        result.root = result.cells[0]
        return result
    }
    /**
     * Deserialize cell
     * @param {BitStream} stream Bitstream of cells
     * @param {number}    size   Ref size
     */
    deserializeCell(stream, size) {
        // Approximated TL-B schema
        // cell$_ flags:(## 2) level:(## 1) hash:(## 1) exotic:(## 1) absent:(## 1) refCount:(## 2) 
        let result = {}
        result.flags = stream.readBits(2)
        result.level = stream.readBits(1)
        result.hash = stream.readBits(1)
        result.exotic = stream.readBits(1)
        result.absent = stream.readBits(1)
        result.refCount = stream.readBits(2)

        if (result.absent) {
            throw new Error("Can't deserialize absent cell!")
        }
        result.length = stream.readBits(7)
        result.lengthHasBits = stream.readBits(1)
        result.data = stream.readBits((result.length + result.lengthHasBits) * 8)
        if (result.lengthHasBits) {
            let idx = result.data.byteLength - 1
            for (let x = 0; x < 6; x++) {
                if (result.data[idx] & (1 << x)) {
                    result.data[idx] &= ~(1 << x)
                    break
                }
            }
        }

        result.refs = []
        for (let x = 0; x < result.refCount; x++) {
            result.refs.push(stream.readBits(length))
        }

        return result
    }
    /**
     * Connect to liteservers
     */
    async connect() {

        let cnt = 0;
        if (this.settings['wssProxies'].length === 0) {
            return;
        }

        this.connList = [];
        this.conns = {};
        for (const key in this.config['liteservers']) {
            const uri = this.settings['wssProxies'][this.config['liteservers'][key]['ip']];
            this.newConnection(this.config['liteservers'][key]);
            this.connList.push(uri);
        }
        function shuffle(a) {
            for (let i = a.length - 1; i > 0; i--) {
                const j = Math.floor(Math.random() * (i + 1));
                [a[i], a[j]] = [a[j], a[i]];
            }
            return a;
        }
        this.connList = shuffle(this.connList);
        this.connCurrent = 0;

        const maxConnections = Math.min(this.settings.maxConnections, this.connList.length);
        this.connPending = 0;
        this.promiseList = [];

        await new Promise((resolve, reject) => {

            this.connectionPromise = { resolve, reject };

            for (let i = 0; i < maxConnections; i++) {
                this.promiseList.push(this.nextConnection());
            }
        });
    }

    newConnection(liteserver) {
        const uri = this.settings['wssProxies'][liteserver['ip']];

        if (this.conns[uri])
            return this.conns[uri];

        let conn = {
            uri,
            config: liteserver,
            lastConnected: 0,
            lastAction: 0,
            failCount: 0,
            status: 'disconnected',
            connection: new ADNLConnection(this.getTL(), liteserver['id'], uri)
        };

        this.conns[uri] = conn;

        return conn;
    }

    getActiveConnection() {
        let l = [];
        for (let i in this.conns) {
            if (this.conns[i].status === 'connected')
                l.push(this.conns[i]);
        }
        return l[fastRandomInt(l.length)];
    }

    /*
    connect to next server
    */
    async nextConnection() {
        if (!this.conns || !this.connList)
            return;

        const whoami = this.connPending;
        this.connPending += 1;

        for (let i = 0; i < this.connList.length; i++) {
            //console.log('Pending connection ' + whoami, ': search for next (from ' + this.connCurrent + '/' + this.connList.length + ')');

            if (this.connCurrent >= this.connList.length) {
                this.connCurrent = 0;
            }

            const uri = this.connList[this.connCurrent];
            this.connCurrent += 1;
            let conn = this.conns[uri];

            if (!conn || conn.status !== 'disconnected')
                continue;

            conn.status = 'pending';
            const tm = new Date().getTime() - conn.lastAction;
            if (tm < 20000) {
                //console.log('Pending connection ' + whoami, ': wait ' + (30000 - tm) + 'ms for connection to ' + conn.uri);
                await (new Promise(resolve => setTimeout(resolve, 30000 - tm)));
            }

            //console.log('Pending connection ' + whoami, ': connecting to ' + conn.uri);

            const res = await this.connectToServer(conn.uri);
            if (res) {
                console.log('Pending connection ' + whoami, ': connected to ' + conn.uri + `: server diff is ${conn.tm.lastUtime - conn.tm.serverTime}`);
                if (this.connectionPromise) {
                    this.connectionPromise.resolve();
                    this.connectionPromise = undefined;
                }
                return;
            }
            console.log('Pending connection ' + whoami, ': connect to ' + conn.uri + ' failed');
        }

        this.connPending -= 1;

        console.log('Pending connection ' + whoami, ': connect to server failed');

        if (this.connPending === 0 && this.connectionPromise) {
            this.connectionPromise.reject(Error('Connection giveup'));
            this.connectionPromise = undefined;
        }

    }

    async connectionLost(uri) {
        if (!this.conns[uri])
            return;

        console.log(uri, 'disconnected');
        //this.conns[uri].lastAction = new Date().getTime();
        this.conns[uri].status = 'disconnected';
        this.conns[uri].connection = undefined;

        await this.nextConnection();
    }

    async connectToServer(uri) {

        let conn = this.conns[uri];

        if (!conn)
            return false;

        try {
            conn.connection = new ADNLConnection(this.getTL(), conn.config['id'], conn.uri);
            await conn.connection.connect();

            conn.lastConnected = conn.lastAction = new Date().getTime();
            conn.status = 'requestInfo';

            conn.masterchainInfo = await this.getMasterchainInfo(conn);

            conn.server = {
                version: conn.masterchainInfo.version,
                capabilities: conn.masterchainInfo.capabilities
            }
            //console.log(`Server version is ${conn.server.version >> 8}.${conn.server.version & 0xFF}, capabilities ${conn.server.capabilities}`)

            const ok = (conn.server.version >= this.min_ls_version) && !(~conn.server.capabilities[0] & this.min_ls_capabilities);
            if (!ok) {
                throw Error(`Server version is too old (at least ${this.min_ls_version >>8}.${this.min_ls_version & 0xFF} with capabilities ${this.min_ls_capabilities} required), some queries are unavailable!`);
            }

            conn.tm = {
                lastUtime: conn.masterchainInfo.last_utime,
                serverTime: conn.masterchainInfo.now,
                gotServerTimeAt: Date.now() / 1000 | 0
            }
            //console.log(`Server time is ${conn.tm.serverTime}, last known block timestamp ${conn.tm.lastUtime}`);

            const last_utime = conn.masterchainInfo.last_utime || 0;
            const last = conn.masterchainInfo.last;

            if (last_utime > conn.tm.serverTime) {
                throw Error(`server claims to have a masterchain block  created at ${last_utime} (${last_utime - conn.tm.serverTime} seconds in the future)`);
            } else if (last_utime < conn.tm.serverTime - 60) {
                throw Error(`server appears to be out of sync: its newest masterchain block is created at ${last_utime} (${conn.tm.serverTime - last_utime} seconds ago according to the server's clock)`);
            } else if (Math.abs(last_utime - conn.tm.gotServerTimeAt) > 60) {
                throw Error(`either the server is out of sync, or the local clock is set incorrectly: the newest masterchain block known to server is created at ${last_utime} (${conn.tm.serverTime - conn.tm.gotServerTimeAt} seconds ago according to the local clock)`);
            }

            const ourZeroState = {
                _: "tonNode.zeroStateIdExt",
                file_hash: this.config.validator.zero_state.file_hash,
                root_hash: this.config.validator.zero_state.root_hash,
                workchain: this.config.validator.zero_state.workchain
            };
            const serverZeroState = conn.masterchainInfo.init;
            if (!deepEqual(serverZeroState, ourZeroState)) {
                console.error("Zerostate invalid: should be ", ourZeroState, " is ", serverZeroState);
                throw Error("Zerostate invalid");
            }

            conn.connection.onClose = () => this.connectionLost(conn.uri);

            conn.status = 'connected';
            return true;
        } catch (e) {
            console.log(conn.uri, 'Connect failed:', e);
            conn.status = 'disconnected';
            conn.connection = undefined;
            conn.failCount += 1;
            conn.lastAction = new Date().getTime();
            return false;
        }

    }
    /**
     * Get masterchain info (equivalent to `last`)
     * @returns {Object} liteServer.masterchainInfoExt or liteServer.masterchainInfo, depending on liteserver version
     */
    last(conn) {
        return this.getMasterchainInfo(conn)
    }
    /**
     * Get masterchain info (equivalent to `last`)
     * @returns {Object} liteServer.masterchainInfoExt or liteServer.masterchainInfo, depending on liteserver version
     */
    async getMasterchainInfo(conn) {
        //const mode = (this.server.capabilities[0] & 2) ? 0 : -1;
        const info = await this.methodCall('liteServer.getMasterchainInfoExt', {
                mode: 0,
            }, conn
        )
        //this._parseMasterchainInfo(info) // Reduce clutter for abstraction methods

        return info
    }
    /**
     * Request block by ID
     * @param {Object} id Block ID
     */
    async requestBlock(id, conn) {
        const block = await this.methodCall('liteServer.getBlock', {
            id
        }, conn)
        if (!deepEqual(block.id, id)) {
            console.error(block, id)
            throw new Error('Got wrong block!')
        }
        //await this._registerMasterchainBlock(block.id, block.data)

        return block
    }
    /**
     * Get server version
     * @returns {Object}
     */
    async getVersion(conn) {
        const server = await this.methodCall('liteServer.getVersion', {}, conn)
        //this._setServerVersion(server)
        //this._setServerTime(server.now)
        return server
    }
    /**
     * Get server time
     * @returns {Object}
     */
    async getTime(conn) {
        const time = await this.methodCall('liteServer.getTime', {}, conn)
        //this._setServerTime(time.now)
        return time
    }



    // Parser functions
    /*
    _setServerVersion(server) {
        this.server = server
        this.server.ok = (server.version >= this.min_ls_version) && !(~server.capabilities[0] & this.min_ls_capabilities);
    }
    _setServerTime(time) {
        this.serverTime = time
        this.gotServerTimeAt = Date.now() / 1000 | 0
    }
    _parseMasterchainInfo(info) {
        const last = info.last
        const zero = info.init
        const last_utime = info.last_utime || 0
        if (info['_'] === 'liteServer.masterchainInfoExt') {
            this._setServerVersion(info)
            this._setServerTime(info.now)

            if (last_utime > this.serverTime) {
                console.error("server claims to have a masterchain block", last, `created at ${last_utime} (${last_utime - this.serverTime} seconds in the future)`)
            } else if (last_utime < this.serverTime - 60) {
                console.error("server appears to be out of sync: its newest masterchain block is", last, `created at ${last_utime} (${server_now - last_utime} seconds ago according to the server's clock)`)
            } else if (last_utime < this.gotServerTimeAt - 60) {
                console.error("either the server is out of sync, or the local clock is set incorrectly: the newest masterchain block known to server is", last, `created at ${last_utime} (${server_now - this.gotServerTimeAt} seconds ago according to the local clock)`)
            }
        }

        let myZeroState;
        if (this.zeroState) {
            myZeroState = this.zeroState
        } else {
            myZeroState = this.config.validator.zero_state
            myZeroState._ = 'tonNode.zeroStateIdExt'
            delete myZeroState.shard
            delete myZeroState.seqno
        }

        if (!deepEqual(zero, myZeroState)) {
            console.log(this.config)
            console.error("Zerostate changed: should be", myZeroState, "is", zero)
            throw new Error("Zerostate changed!")
        }
        if (!this.zeroState) {
            this.zeroState = zero
            //this._registerBlockId(zero)
            console.log("Zerostate OK!")
        }

        //this._registerBlockId(last)
        if (!this.lastMasterchainId) {
            this.lastMasterchainId = last
            this.requestBlock(last)
        } else if (this.lastMasterchainId.seqno < last.seqno) {
            this.lastMasterchainId = last
        }
        console.log(`Last masterchain block ID known to server is `, last, last_utime ? `created at ${last_utime}` : '')
    }

    // Unused for now
    _registerBlockId(id) {
        for (const block in this.knownBlockIds) {
            if (deepEqual(block, id)) {
                return
            }
        }
        this.knownBlockIds.push(id)
    }
    async _registerMasterchainBlock(id, data) {
        const hash = new Uint32Array(await this.crypto.sha256(data))
        if (!bufferViewEqual(id.file_hash, hash)) {
            console.error(`File hash mismatch for block `, id, `expected ${id.file_hash}, got ${hash}`)
            throw new Error(`File hash mismatch!`)
        }
        //this._registerBlockId(id)
        // Here we should save the block to storage
    }
    */

    /**
     * Call liteserver method
     * @param {string} method Liteserver method name
     * @param {Object} args   Arguments
     */
    methodCall(method, args = {}, conn=undefined) {
        args['_'] = method
        args['id'] = args['id'] || this.lastMasterchainId

        let data
        data = this.TLParser.serialize(new Stream, args).bBuf
        data = this.TLParser.serialize(new Stream, {
            _: 'liteServer.query',
            data
        }).bBuf

        if (conn !== undefined) {
            return conn.connection.query(data);
        } else {
            conn = this.getActiveConnection();
            if (conn === undefined)
                throw Error('No active connections');
            return conn.connection.query(data);
        }
        //return this.connections[fastRandomInt(this.connections.length)].query(data)
    }

    /**
     * Call RLDP method
     * @param {string} method RLDP method name
     * @param {Object} args   Arguments
     */
    rldpCall(method, args = {}, conn=undefined) {
        args['_'] = method

        let data
        data = this.TLParser.serialize(new Stream, args).bBuf

        if (conn !== undefined) {
            return conn.connection.queryRLDP(data);
        } else {
            conn = this.getActiveConnection();
            if (conn === undefined)
                throw Error('No active connections');
            return conn.connection.queryRLDP(data);
        }

        //return this.connections[fastRandomInt(this.connections.length)].queryRLDP(data)
    }
    getTL() {
        return this.TLParser
    }
}

export default Lite;