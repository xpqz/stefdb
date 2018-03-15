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
            leaves = await this.openRevs(id);
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

    /**
     * Generate the ops required for a document update.
     *
     * @param doc A document containing both _id and _rev, referring to
     * the revision we want to update.
     */
    private async update(doc: Document): Promise<BulkOperation[]> {
        const parentRev = doc._rev;
        const parentDocKey = makeKey(this.prefix.doc, doc._id, parentRev);
        const parentTreeKey = makeKey(this.prefix.revs, doc._id, parentRev);
        let node: Node;
        try {
            node = JSON.parse(await this.db.get(parentDocKey));
        } catch(err) {
            throw new Error(`[update] no such document: ${parentDocKey}`);
        }

        if (!node.leaf) {
            throw new Error("[update] document update conflict");
        }

        // console.log(`Updating rev: ${parentRev}`);

        // We're a leaf. Generate next-gen rev id for the update.
        const parentGen = parseInt(parentRev.split('-')[0]);
        const newRev = makeRev(parentGen+1, doc);
        doc._rev = newRev;

        // console.log(`New rev: ${newRev}`);

        // Store new rev
        const newRevKey = makeKey(this.prefix.doc, doc._id, newRev);
        const newRevTreeKey = makeKey(this.prefix.revs, doc._id, newRev);
        let ops: BulkOperation[] = [{
            type: 'put',
            key: newRevKey,
            value: JSON.stringify({
                parent: parentDocKey,
                leaf: true,
                body: doc
            })
        }];

        // Parent is no longer a leaf, and body is no longer needed
        ops.push({
            type: 'put',
            key: parentDocKey,
            value: JSON.stringify({
                leaf: false,
                body: null
            })
        });

        // Ancestry relation
        let revs: Revisions;
        try {
            revs = JSON.parse(await this.db.get(parentTreeKey));
        } catch(err) {
            throw new Error(`[update] No tree structure found for ${parentTreeKey}`);
        }

        ops.push({
            type: 'put',
            key: newRevTreeKey,
            value: JSON.stringify({
                start: parentGen+1,
                ids: [newRev].concat(revs.ids)
            })
        });

        // Open revs -- remove parent, add new
        let leaves: Leaves;
        try {
            leaves = await this.openRevs(doc._id);
        } catch (err) {
            throw new Error(`[update] No open revs found for ${doc._id}`);
        }

        const openRevsKey = makePrefixedKey(this.prefix.leaves, doc._id);
        const index = leaves.open_revs.indexOf(parentRev);
        if (index > -1) {
            leaves.open_revs.splice(index, 1);
        } else {
            throw new Error(`[update] Parent ${parentRev} not in open revisions for key: ${openRevsKey}`);
        }
        leaves.open_revs.push(newRev);

        ops.push({
            type: 'put',
            key: openRevsKey,
            value: JSON.stringify(leaves)
        });

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
        // 1. Store doc itself
        const docKey = makeKey(this.prefix.doc, doc._id, doc._rev);
        let ops: BulkOperation[] = [{
            type: 'put',
            key: docKey,
            value: JSON.stringify({
                leaf: true,
                body: doc
            })
        }];

        // 2. Update the document revisions list
        const [genS, hash] = doc._rev.split('-');
        const treeKey = makeKey(this.prefix.revs, doc._id, doc._rev)
        let leaves: Leaves = { open_revs: [doc._rev] };
        if (newEdits) {
            ops.push({
                type: 'put',
                key: treeKey,
                value: JSON.stringify({
                    start: 1,
                    ids: [hash]
                })
            });
        } else {
            ops.push({
                type: 'put',
                key: treeKey,
                value: JSON.stringify({
                    start: parseInt(genS),
                    ids: doc._revisions.ids
                })
            });

            let currentLeaves: Leaves;
            try {
                currentLeaves = await this.openRevs(doc._id);
            } catch (err) {
                throw err;
            }
            leaves.open_revs = leaves.open_revs.concat(currentLeaves.open_revs);
        }

        ops.push({
            type: 'put',
            key:  makePrefixedKey(this.prefix.leaves, doc._id),
            value: JSON.stringify(leaves)
        });

        return ops;
    }

    /**
     * Four cases:
     *   1. { "foo": "baz" }
     *      New document. Generate id and 1-gen rev. Key must not exist.
     *   2. { "_id": ..., "bar": "baz" }
     *      New document with a specified id. Generate 1-gen rev. Key
     *      must not exist.
     *   3. { "_id": ..., "_rev": ..., "bar": "baz" }
     *      New revision. Check that {_id, _rev} is a leaf. New rev gen+1
     *   4. { "_id": ..., "_rev": ..., "_deleted": true }
     *      New tombstone. Check that {_id, _rev} is a leaf. New rev gen+1
     *
     * @param doc document to be written to the database
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
        let statements = [] as BulkOperation[];
        let response = [] as DocumentMeta[];
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
                    response.push({
                        id: id,
                        rev: rev
                    });
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
        let key: string;
        if (typeof metaOrID === "string") {
            let winningRev = await this.winner(metaOrID);
            try {
                winningRev = await this.winner(metaOrID);
            } catch (err) {
                throw err;
            }
            key = makeKey(this.prefix.doc, metaOrID, winningRev);
        } else {
            key = makeKey(this.prefix.doc, metaOrID.id, metaOrID.rev);
        }

        try {
            return JSON.parse(await this.db.get(key) as any).body;
        } catch (err) {
            throw err;
        }
    }

    public async openRevs(id: string): Promise<Leaves> {
        const key = makePrefixedKey(this.prefix.leaves, id);
        try {
            return JSON.parse(await this.db.get(key));
        } catch (err) {
            throw new Error(`[openRevs] key not found: ${key}`);
        }
    }

    public async getRevisions(meta: DocumentMeta): Promise<Revisions> {
        const key = makeKey(this.prefix.revs, meta.id, meta.rev);
        try {
            return JSON.parse(await this.db.get(key));
        } catch (err) {
            throw new Error(`[getRevisions] key not found: ${key}`);
        }
    }
}
