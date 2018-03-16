const { StefDB, DocumentMeta } = require('../dist/stefdb');
const { expect } = require('chai');
const mocha = require('mocha');

const db = new StefDB('./tmp-db');

describe('Write method', function() {
    it('should save a document successfully', async function() {
        const meta = await db.write({"name": "bob"});
        expect(meta).to.have.property('rev').with.lengthOf(34);

        const leaves = await db.getLeaves(meta.id);
        expect(leaves.open_revs).to.have.lengthOf(1);

        const revs = await db.getRevisions(meta.id, meta.rev);

        expect(revs.start).to.equal(1);
    });
});

describe('Read method', function() {
    it('should return a document successfully', async function() {
        const meta = await db.write({"name": "carrie"});
        const doc = await db.read(meta);
        expect(doc._rev).to.equal(meta.rev);
    });

    it('should return a winner document successfully', async function() {
        const meta = await db.write({"name": "carrie"});
        const doc = await db.read(meta.id);
        expect(doc._rev).to.equal(meta.rev);
    });
});

describe('Update', function() {
    it('should increase the rev generation', async function() {
        const meta = await db.write({"name": "bob"});
        const meta2 = await db.write({
            "_id": meta.id,
            "_rev": meta.rev,
            "name": "eric"
        });
        const doc2 = await db.read(meta2);

        let match = doc2._rev.indexOf('2-');
        expect(match).to.equal(0);

        const leaves = await db.getLeaves(meta.id);
        expect(leaves.open_revs[0]).to.equal(meta2.rev);
    });

    it('should allow read() to fetch on winner', async function() {
        const meta = await db.write({"name": "bob"});
        await db.write({
            "_id": meta.id,
            "_rev": meta.rev,
            "name": "eric"
        });

        const doc2 = await db.read(meta.id);
        let match = doc2._rev.indexOf('2-')
        expect(match).to.equal(0);
    });
});

describe('BulkWrite method', function() {
    it('should write multiple docs at once', async function() {
        const meta = await db.bulkWrite([
            {"name": "bob"},
            {"name": "eric"},
            {"name": "sally"},
        ]);
        expect(meta).to.have.lengthOf(3);
    });

    it('should be able to branch the doc tree', async function() {
        const meta = await db.bulkWrite([
            {"name": "bob"},
            {"name": "eric"},
            {"name": "sally"},
        ]);
        await db.write({
            "_id": meta[0].id,
            "_rev": meta[0].rev,
            "name": "robert"
        });
        const parentRevHash = meta[0].rev.split('-')[1];
        const newLeaves = await db.bulkWrite([{
            "_id": meta[0].id,
            "_rev": '2-590a80b371bb6f2f358b0c2d492bdc95',
            "_revisions": {
                start: 2,
                ids: ['590a80b371bb6f2f358b0c2d492bdc95', parentRevHash]
            },
            "name": "rob"
        }], false);

        const leaves = await db.getLeaves(meta[0].id);
        expect(leaves.open_revs).to.have.lengthOf(2);

        const revs = await db.getRevisions(
            meta[0].id,
            '2-590a80b371bb6f2f358b0c2d492bdc95'
        );
        expect(revs.start).to.equal(2);
        expect(revs.ids[0]).to.equal('590a80b371bb6f2f358b0c2d492bdc95');
        expect(revs.ids[1]).to.equal(parentRevHash);
    });
});
