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
    [propName: string]: any;
}

interface Ancestor {
    readonly ancestors: string[];
}

interface OpenRevs {
    readonly open_revs: string[];
}

interface Node {
    parent: string | null;
    leaf: boolean;
    body: Document;
}

interface BulkOperation {
    readonly type: string;
    readonly key: string;
    readonly value: string;
}

export interface DocumentMeta {
    readonly id: string;
    readonly rev: string;
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
        tree: 'tree',
        leaves: 'leaves'
    };

    constructor(dbName: string) {
        this.db = levelup(leveldown(dbName));
    }

    /**
     * Generate the ops required for a document update.
     *
     * @param doc A document containing both _id and _rev, referring to
     * the revision we want to update.
     */
    private async update(doc: Document): Promise<BulkOperation[]> {
        const parentDocKey = makeKey(this.prefix.doc, doc._id, doc._rev);
        const parentTreeKey = makeKey(this.prefix.tree, doc._id, doc._rev);
        let node: Node;
        try {
            node = JSON.parse(await this.db.get(parentDocKey));
        } catch(err) {
            throw "no such document";
        }

        if (!node.leaf) {
            throw "document update conflict";
        }

        // We're a leaf. Generate next-gen rev id for the update.
        const [gen, _] = doc._rev.split('-');
        const newRev = makeRev(parseInt(gen)+1, doc);
        doc._rev = newRev;

        // Store new rev
        const newRevKey = makeKey(this.prefix.doc, doc._id, newRev);
        const newRevTreeKey = makeKey(this.prefix.tree, doc._id, newRev);
        let ops: BulkOperation[] = [{
            type: 'put',
            key: newRevKey,
            value: JSON.stringify({
                parent: parentDocKey,
                leaf: true,
                body: doc
            })
        }];

        // Parent is no longer a leaf
        ops.push({
            type: 'put',
            key: parentDocKey,
            value: JSON.stringify({
                parent: node.parent,
                leaf: false,
                body: {}
            })
        });

        // Ancestry relation
        let parentalAncestors: Ancestor;
        try {
            parentalAncestors = JSON.parse(
                await this.db.get(parentTreeKey)
            );
        } catch(err) {
            throw "No tree structure found";
        }

        ops.push({
            type: 'put',
            key: newRevTreeKey,
            value: JSON.stringify({
                ancestors: [parentDocKey].concat(
                    parentalAncestors.ancestors
                )
            })
        });

        // Open revs -- remove parent, add new
        const openRevsKey = makePrefixedKey(
            this.prefix.leaves,
            doc._id
        );

        let openRevs: OpenRevs;
        try {
            openRevs = JSON.parse(await this.db.get(openRevsKey));
        } catch(err) {
            throw "No open revs found";
        }

        const index = openRevs.open_revs.indexOf(parentDocKey);
        if (index > -1) {
            openRevs.open_revs.splice(index, 1);
        } else {
            throw "Parent not in open revisions";
        }
        openRevs.open_revs.push(newRevKey);

        ops.push({
            type: 'put',
            key: openRevsKey,
            value: JSON.stringify(openRevs)
        });

        console.log(ops);

        return ops;
    }

    /**
     * Generate the ops required for a new document create.
     *
     * @param doc A document containing no _id.
     */
    private async create(doc: Document): Promise<BulkOperation[]> {
        doc._id = makeID();
        doc._rev = makeRev(1, doc);
        const key = makeKey(this.prefix.doc, doc._id, doc._rev);
        let ops: BulkOperation[] = [{
            type: 'put',
            key: key,
            value: JSON.stringify({
                parent: null,
                leaf: true,
                body: doc
            })
        }];

        // Indentity ancestry relation
        ops.push({
            type: 'put',
            key: makeKey(this.prefix.tree, doc._id, doc._rev),
            value: JSON.stringify({
                ancestors: [],
            })
        });

        // Open revs
        ops.push({
            type: 'put',
            key: makePrefixedKey(this.prefix.leaves, doc._id),
            value: JSON.stringify({
                open_revs: [key],
            })
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
    private async generateBulkOperation(doc: Document): Promise<BulkOperation[]> {
        let docClone = Object.assign({}, doc);
        let ops: BulkOperation[] = [];

        if (docClone._id && docClone._rev) { // Update or delete
            try {
                ops = ops.concat(await this.update(docClone));
            } catch (err) {
                throw err;
            }
        } else if (!docClone._rev) { // Create
            if (!docClone._id) {
                docClone._id = makeID();
            }
            try {
                ops = ops.concat(await this.create(docClone));
            } catch (err) {
                throw err;
            }
        }

        return ops;
    }

    public async bulkWrite(docs: Document[]): Promise<DocumentMeta[]> {
        let statements = [] as BulkOperation[];
        let response = [] as DocumentMeta[];
        for (const doc of docs) {
            let ops: BulkOperation[] = [];
            try {
                ops = await this.generateBulkOperation(doc);
            } catch (err) {
                throw err;
            }
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

    public async read(meta: DocumentMeta): Promise<Document> {
        const key = makeKey(this.prefix.doc, meta.id, meta.rev);
        try {
            return JSON.parse(await this.db.get(key) as any).body;
        } catch (err) {
            throw err;
        }
    }
}
