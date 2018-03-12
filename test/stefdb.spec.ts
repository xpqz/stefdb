import { StefDB } from '../stefdb';
import { expect } from 'chai';
import 'mocha';
import 'chai-string';

const db = new StefDB('./tmp-db');

describe('Create method', function() {
    it('should save a document successfully', async function() {
        const meta = await db.write({"name": "bob"});
        expect(meta).to.have.property('rev').with.lengthOf(34);
    });
});

describe('Read method', function() {
    it('should return a document successfully', async function() {
        const meta = await db.write({"name": "carrie"});
        const doc = await db.read(meta);
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
        const match = doc2._rev.indexOf('2-')
        expect(match).to.equal(0);
    });
});
