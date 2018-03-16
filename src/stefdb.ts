import * as _leveldown  from 'leveldown';
import * as _levelup    from 'levelup';
import * as uuidv4 from 'uuid/v4';
import { Md5 } from 'ts-md5/dist/md5';

const leveldown = (<any>_leveldown).default || _leveldown
const levelup   = (<any>_levelup).default   || _levelup

export interface Document {
    _id?: string;
    _rev?: string;
    _deleted?: boolean;
    _revisions?: Revisions;
    [propName: string]: any;
}

// Last is parent. Self is first. For root, single entry.
interface Revisions {
    start: number;
    ids: string[];
}

interface Leaves {
    open_revs: string[];
}

interface Node {
    readonly leaf: boolean;
    readonly body: Document;
}

interface BulkOperation {
    readonly type: string;
    readonly key: string;
    readonly value: string;
}

export interface DocumentMeta {
    id: string;
    rev: string;
}

const makePrefixedKey = (prefix: string, id: string): string => {
    return `${prefix}!${id}`;
}

const makeID = (): string => {
    return Md5.hashAsciiStr(uuidv4()) as string;
}

const makeRev = (gen: number, doc: Document): string => {
    return `${gen}-${Md5.hashAsciiStr(JSON.stringify(doc)) as string}`;
}

const makeKey = (prefix: string, id: string, rev: string): string => {
    return `${prefix}!${id}!${rev}`;
}

export class StefDB {
    private db: any;
    private readonly prefix = {
        doc: 'doc',
        revs: 'revs',
        leaves: 'leaves'
    };

    constructor(dbName: string) {
        this.db = levelup(leveldown(dbName));
    }

    private async winner(id: string): Promise<string> {
        let leaves: Leaves;
        try {
            leaves = await this.getLeaves(id);
        } catch(err) {
            throw new Error(`[winner] No open revs found for ${id}`);
        }

        // Sort on gen first, then lexicographically on rev.
        leaves.open_revs.sort((a, b): number => {
            const [gen1s, rev1] = a.split('-');
            const [gen2s, rev2] = b.split('-');

            const gen1i = parseInt(gen1s);
            const gen2i = parseInt(gen2s);

            if (gen1i > gen2i) {
                return -1;
            }

            if (gen1i < gen2i) {
                return 1;
            }

            if (rev1 >= rev2) {
                return -1
            }

            return 1
        });

        return leaves.open_revs[0];
    }

    private storeDocOperation(id: string, rev: string, doc: Document | null): BulkOperation {
        return {
            type: 'put',
            key: makeKey(this.prefix.doc, id, rev),
            value: JSON.stringify({leaf: doc !== null, body: doc})
        };
    }

    private storeRevsOperation(id: string, rev: string, gen: number, ids: string[]): BulkOperation {
        return {
            type: 'put',
            key: makeKey(this.prefix.revs, id, rev),
            value: JSON.stringify({start: gen, ids: ids})
        };
    }

    private storeLeavesOperation(id: string, leaves: Leaves): BulkOperation {
        return {
            type: 'put',
            key: makePrefixedKey(this.prefix.leaves, id),
            value: JSON.stringify(leaves)
        };
    }

    /**
     * Generate the ops required for a document update.
     *
     * @param doc A document containing both _id and _rev, referring to
     * the revision we want to update.
     */
    private async update(doc: Document): Promise<BulkOperation[]> {
        const parentRev = doc._rev;

        try {
            if (!await this.isLeaf(doc._id, parentRev)) {
                throw new Error("[update] document update conflict");
            }
        } catch (err) {
            throw err;
        }

        // We're a leaf. Generate next-gen rev id for the update.
        const parentGen = parseInt(parentRev.split('-')[0]);
        const newRev = makeRev(parentGen+1, doc);
        doc._rev = newRev;

        // console.log(`New rev: ${newRev}`);

        // Store new rev, and update parent to no longer be a leaf
        let ops = [
            this.storeDocOperation(doc._id, newRev, doc),
            this.storeDocOperation(doc._id, parentRev, null)
        ];

        // Ancestry relation
        let revs: Revisions;
        try {
            revs = await this.getRevisions(doc._id, parentRev);
        } catch(err) {
            throw err;
        }
        ops.push(this.storeRevsOperation(
            doc._id,
            newRev,
            parentGen+1,
            [newRev].concat(revs.ids)
        ));

        // Leaves list -- remove parent, add new rev
        let leaves: Leaves;
        try {
            leaves = await this.getLeaves(doc._id);
        } catch (err) {
            throw err;
        }

        const index = leaves.open_revs.indexOf(parentRev);
        if (index > -1) {
            leaves.open_revs.splice(index, 1);
        } else {
            throw new Error(`[update] Parent ${parentRev} not in open revisions for id: ${doc._id}`);
        }
        leaves.open_revs.push(newRev);

        ops.push(this.storeLeavesOperation(doc._id, leaves));

        return ops;
    }

    /**
     * Generate the ops required for a new document create:
     *
     * 1. Store the new document revision itself
     * 2. Update the document revisions list
     * 3. Update the list of open leaves
     *
     * @param doc A new document, or an existing document replicated in
     *            from a different database.
     */
    private async create(doc: Document, newEdits: boolean = true): Promise<BulkOperation[]> {
        if (newEdits) {
            doc._rev = makeRev(1, doc);
        }

       // Store new document revision
       let ops = [ this.storeDocOperation(doc._id, doc._rev, doc) ];

        // 2. Update the document revisions list
        const [genS, hash] = doc._rev.split('-');
        let leaves: Leaves = { open_revs: [doc._rev] };
        if (newEdits) {
            ops.push(this.storeRevsOperation(doc._id, doc._rev, 1, [hash]));
        } else {
            ops.push(this.storeRevsOperation(doc._id, doc._rev, parseInt(genS), doc._revisions.ids));
            try {
                const currentLeaves = await this.getLeaves(doc._id);
                leaves.open_revs = leaves.open_revs.concat(currentLeaves.open_revs);
            } catch (err) {
                throw err;
            }
        }

        // 3. Update the list of open leaves
        ops.push(this.storeLeavesOperation(doc._id, leaves));

        return ops;
    }

    /**
     * @param doc document to be written to the database
     * @param newEdits true for normal operation, false for replicator mode
     */
    private async generateBulkOperation(doc: Document, newEdits: boolean): Promise<BulkOperation[]> {
        let docClone = Object.assign({}, doc);
        let op: Promise<BulkOperation[]>;

        if (newEdits) { // Normal mode
            if (docClone._id && docClone._rev) { // Update/delete
                op = this.update(docClone);
            } else if (!docClone._rev) { // Create
                if (!docClone._id) { // db-assigned _id
                    docClone._id = makeID();
                }
                op = this.create(docClone, newEdits);
            }
        } else { // Replicator mode
            if (!docClone._id || !docClone._rev) {
                throw new Error("expected both _id and _rev");
            }
            op = this.create(docClone, newEdits);
        }

        let ops: BulkOperation[] = [];
        try {
            ops = await op;
        } catch (err) {
            throw err;
        }

        return ops;
    }

    public async bulkWrite(docs: Document[], newEdits: boolean = true): Promise<DocumentMeta[]> {
        let statements: BulkOperation[] = [];
        let response: DocumentMeta[] = [];
        for (const doc of docs) {
            let ops: BulkOperation[] = [];
            try {
                ops = await this.generateBulkOperation(doc, newEdits);
            } catch (err) {
                throw err;
            }
            // Pick out the _id/_rev pairs from document revision writes.
            for (const op of ops) {
                const [prefix, id, rev] = op.key.split('!');
                if (prefix === this.prefix.doc) {
                    response.push({id: id, rev: rev});
                }
            }
            statements = statements.concat(ops);
        }

        try {
            await this.db.batch(statements);
        } catch (err) {
            throw err;
        }

        return response;
    }

    public async write(doc: Document): Promise<DocumentMeta> {
        let meta: DocumentMeta[];
        try {
            meta = await this.bulkWrite([doc]);
        } catch (err) {
            throw err;
        }

        return meta[0];
    }

    public async read(metaOrID: DocumentMeta | string): Promise<Document> {
        let id: string;
        let rev: string;
        if (typeof metaOrID === "string") {
            id = metaOrID;
            try {
                rev = await this.winner(metaOrID);
            } catch (err) {
                throw err;
            }
        } else {
            id = metaOrID.id;
            rev = metaOrID.rev;
        }

        try {
            return (await this.getNode(id, rev)).body;
        } catch (err) {
            throw err;
        }
    }

    private async isLeaf(id: string, rev: string): Promise<boolean> {
        try {
            const node = await this.getNode(id, rev);
            return node.leaf;
        } catch (err) {
            throw err;
        }
    }

    private async getNode(id: string, rev: string): Promise<Node> {
        const docKey = makeKey(this.prefix.doc, id, rev);
        try {
            return JSON.parse(await this.db.get(docKey));
        } catch (err) {
            throw new Error(`[getNode] no node found for {id:${id}, rev:${rev}}`);
        }
    }

    public async getLeaves(id: string): Promise<Leaves> {
        const key = makePrefixedKey(this.prefix.leaves, id);
        try {
            return JSON.parse(await this.db.get(key));
        } catch (err) {
            throw new Error(`[getLeaves] no leaves found for id ${id}`);
        }
    }

    public async getRevisions(id: string, rev: string): Promise<Revisions> {
        const key = makeKey(this.prefix.revs, id, rev);
        try {
            return JSON.parse(await this.db.get(key));
        } catch (err) {
            throw new Error(`[getRevisions] No revisions found for {id:${id}, rev:${rev}}`);
        }
    }
}
