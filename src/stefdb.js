"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = y[op[0] & 2 ? "return" : op[0] ? "throw" : "next"]) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [0, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
exports.__esModule = true;
var _leveldown = require("leveldown");
var _levelup = require("levelup");
var uuidv4 = require("uuid/v4");
var md5_1 = require("ts-md5/dist/md5");
var leveldown = _leveldown["default"] || _leveldown;
var levelup = _levelup["default"] || _levelup;
var makePrefixedKey = function (prefix, id) {
    return prefix + "!" + id;
};
var makeID = function () {
    return md5_1.Md5.hashAsciiStr(uuidv4());
};
var makeRev = function (gen, doc) {
    return gen + "-" + md5_1.Md5.hashAsciiStr(JSON.stringify(doc));
};
var makeKey = function (prefix, id, rev) {
    return prefix + "!" + id + "!" + rev;
};
var StefDB = /** @class */ (function () {
    function StefDB(dbName) {
        this.prefix = {
            doc: 'doc',
            tree: 'tree',
            leaves: 'leaves'
        };
        this.db = levelup(leveldown(dbName));
    }
    StefDB.prototype.winner = function (id) {
        return __awaiter(this, void 0, void 0, function () {
            var revs, err_1;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 2, , 3]);
                        return [4 /*yield*/, this.openRevs(id)];
                    case 1:
                        revs = _a.sent();
                        return [3 /*break*/, 3];
                    case 2:
                        err_1 = _a.sent();
                        throw "No open revs found";
                    case 3:
                        // Sort on gen first, then lexicographically on rev.
                        revs.open_revs.sort(function (a, b) {
                            var genrev1 = a.split('!')[2];
                            var genrev2 = b.split('!')[2];
                            var _a = genrev1.split('-'), gen1s = _a[0], rev1 = _a[1];
                            var _b = genrev2.split('-'), gen2s = _b[0], rev2 = _b[1];
                            var gen1i = parseInt(gen1s);
                            var gen2i = parseInt(gen2s);
                            if (gen1i > gen2i) {
                                return -1;
                            }
                            if (gen1i < gen2i) {
                                return 1;
                            }
                            if (rev1 >= rev2) {
                                return -1;
                            }
                            return 1;
                        });
                        return [2 /*return*/, revs.open_revs[0]];
                }
            });
        });
    };
    /**
     * Generate the ops required for a document update.
     *
     * @param doc A document containing both _id and _rev, referring to
     * the revision we want to update.
     */
    StefDB.prototype.update = function (doc) {
        return __awaiter(this, void 0, void 0, function () {
            var parentDocKey, parentTreeKey, node, _a, _b, err_2, _c, gen, _, newRev, newRevKey, newRevTreeKey, ops, parentalAncestors, _d, _e, err_3, revs, err_4, openRevsKey, index;
            return __generator(this, function (_f) {
                switch (_f.label) {
                    case 0:
                        parentDocKey = makeKey(this.prefix.doc, doc._id, doc._rev);
                        parentTreeKey = makeKey(this.prefix.tree, doc._id, doc._rev);
                        _f.label = 1;
                    case 1:
                        _f.trys.push([1, 3, , 4]);
                        _b = (_a = JSON).parse;
                        return [4 /*yield*/, this.db.get(parentDocKey)];
                    case 2:
                        node = _b.apply(_a, [_f.sent()]);
                        return [3 /*break*/, 4];
                    case 3:
                        err_2 = _f.sent();
                        throw "no such document";
                    case 4:
                        if (!node.leaf) {
                            throw "document update conflict";
                        }
                        _c = doc._rev.split('-'), gen = _c[0], _ = _c[1];
                        newRev = makeRev(parseInt(gen) + 1, doc);
                        doc._rev = newRev;
                        newRevKey = makeKey(this.prefix.doc, doc._id, newRev);
                        newRevTreeKey = makeKey(this.prefix.tree, doc._id, newRev);
                        ops = [{
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
                        _f.label = 5;
                    case 5:
                        _f.trys.push([5, 7, , 8]);
                        _e = (_d = JSON).parse;
                        return [4 /*yield*/, this.db.get(parentTreeKey)];
                    case 6:
                        parentalAncestors = _e.apply(_d, [_f.sent()]);
                        return [3 /*break*/, 8];
                    case 7:
                        err_3 = _f.sent();
                        throw "No tree structure found";
                    case 8:
                        ops.push({
                            type: 'put',
                            key: newRevTreeKey,
                            value: JSON.stringify({
                                ancestors: [parentDocKey].concat(parentalAncestors.ancestors)
                            })
                        });
                        _f.label = 9;
                    case 9:
                        _f.trys.push([9, 11, , 12]);
                        return [4 /*yield*/, this.openRevs(doc._id)];
                    case 10:
                        revs = _f.sent();
                        return [3 /*break*/, 12];
                    case 11:
                        err_4 = _f.sent();
                        throw "No open revs found";
                    case 12:
                        openRevsKey = makePrefixedKey(this.prefix.leaves, doc._id);
                        index = revs.open_revs.indexOf(parentDocKey);
                        if (index > -1) {
                            revs.open_revs.splice(index, 1);
                        }
                        else {
                            throw "Parent not in open revisions";
                        }
                        revs.open_revs.push(newRevKey);
                        ops.push({
                            type: 'put',
                            key: openRevsKey,
                            value: JSON.stringify(revs)
                        });
                        // console.log(ops);
                        return [2 /*return*/, ops];
                }
            });
        });
    };
    /**
     * Generate the ops required for a new document create.
     *
     * @param doc A document without a _rev, but with _id.
     */
    StefDB.prototype.create = function (doc, newEdits) {
        if (newEdits === void 0) { newEdits = true; }
        return __awaiter(this, void 0, void 0, function () {
            var key, ops;
            return __generator(this, function (_a) {
                if (newEdits) {
                    doc._rev = makeRev(1, doc);
                }
                key = makeKey(this.prefix.doc, doc._id, doc._rev);
                ops = [{
                        type: 'put',
                        key: key,
                        value: JSON.stringify({
                            parent: (newEdits ? null : doc._revisions[0]),
                            leaf: true,
                            body: doc
                        })
                    }];
                // Ancestry relation
                ops.push({
                    type: 'put',
                    key: makeKey(this.prefix.tree, doc._id, doc._rev),
                    value: JSON.stringify({
                        ancestors: (newEdits ? [] : doc._revisions)
                    })
                });
                // Open revs
                ops.push({
                    type: 'put',
                    key: makePrefixedKey(this.prefix.leaves, doc._id),
                    value: JSON.stringify({
                        open_revs: [key]
                    })
                });
                console.log(ops);
                return [2 /*return*/, ops];
            });
        });
    };
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
    StefDB.prototype.generateBulkOperation = function (doc, newEdits) {
        return __awaiter(this, void 0, void 0, function () {
            var docClone, op, ops, err_5;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        docClone = Object.assign({}, doc);
                        if (docClone._id && docClone._rev && newEdits) {
                            op = this.update(docClone);
                        }
                        else if (!docClone._rev) {
                            if (!docClone._id) {
                                docClone._id = makeID();
                            }
                            op = this.create(docClone, newEdits);
                        }
                        ops = [];
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 3, , 4]);
                        return [4 /*yield*/, op];
                    case 2:
                        ops = _a.sent();
                        return [3 /*break*/, 4];
                    case 3:
                        err_5 = _a.sent();
                        throw err_5;
                    case 4: return [2 /*return*/, ops];
                }
            });
        });
    };
    StefDB.prototype.bulkWrite = function (docs, newEdits) {
        if (newEdits === void 0) { newEdits = true; }
        return __awaiter(this, void 0, void 0, function () {
            var statements, response, _i, docs_1, doc, ops, err_6, _a, ops_1, op, _b, prefix, id, rev, err_7;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        statements = [];
                        response = [];
                        _i = 0, docs_1 = docs;
                        _c.label = 1;
                    case 1:
                        if (!(_i < docs_1.length)) return [3 /*break*/, 7];
                        doc = docs_1[_i];
                        ops = [];
                        _c.label = 2;
                    case 2:
                        _c.trys.push([2, 4, , 5]);
                        return [4 /*yield*/, this.generateBulkOperation(doc, newEdits)];
                    case 3:
                        ops = _c.sent();
                        return [3 /*break*/, 5];
                    case 4:
                        err_6 = _c.sent();
                        throw err_6;
                    case 5:
                        // Pick out the _id/_rev pairs from document revision writes.
                        for (_a = 0, ops_1 = ops; _a < ops_1.length; _a++) {
                            op = ops_1[_a];
                            _b = op.key.split('!'), prefix = _b[0], id = _b[1], rev = _b[2];
                            if (prefix === this.prefix.doc) {
                                response.push({
                                    id: id,
                                    rev: rev
                                });
                            }
                        }
                        statements = statements.concat(ops);
                        _c.label = 6;
                    case 6:
                        _i++;
                        return [3 /*break*/, 1];
                    case 7:
                        _c.trys.push([7, 9, , 10]);
                        return [4 /*yield*/, this.db.batch(statements)];
                    case 8:
                        _c.sent();
                        return [3 /*break*/, 10];
                    case 9:
                        err_7 = _c.sent();
                        throw err_7;
                    case 10: return [2 /*return*/, response];
                }
            });
        });
    };
    StefDB.prototype.write = function (doc) {
        return __awaiter(this, void 0, void 0, function () {
            var meta, err_8;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 2, , 3]);
                        return [4 /*yield*/, this.bulkWrite([doc])];
                    case 1:
                        meta = _a.sent();
                        return [3 /*break*/, 3];
                    case 2:
                        err_8 = _a.sent();
                        throw err_8;
                    case 3: return [2 /*return*/, meta[0]];
                }
            });
        });
    };
    StefDB.prototype.read = function (metaOrID) {
        return __awaiter(this, void 0, void 0, function () {
            var key, err_9, _a, _b, err_10;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        if (!(typeof metaOrID === "string")) return [3 /*break*/, 5];
                        _c.label = 1;
                    case 1:
                        _c.trys.push([1, 3, , 4]);
                        return [4 /*yield*/, this.winner(metaOrID)];
                    case 2:
                        key = _c.sent();
                        return [3 /*break*/, 4];
                    case 3:
                        err_9 = _c.sent();
                        throw err_9;
                    case 4: return [3 /*break*/, 6];
                    case 5:
                        key = makeKey(this.prefix.doc, metaOrID.id, metaOrID.rev);
                        _c.label = 6;
                    case 6:
                        _c.trys.push([6, 8, , 9]);
                        _b = (_a = JSON).parse;
                        return [4 /*yield*/, this.db.get(key)];
                    case 7: return [2 /*return*/, _b.apply(_a, [_c.sent()]).body];
                    case 8:
                        err_10 = _c.sent();
                        throw err_10;
                    case 9: return [2 /*return*/];
                }
            });
        });
    };
    StefDB.prototype.openRevs = function (id) {
        return __awaiter(this, void 0, void 0, function () {
            var key, openRevs, _a, _b, err_11;
            return __generator(this, function (_c) {
                switch (_c.label) {
                    case 0:
                        key = makePrefixedKey(this.prefix.leaves, id);
                        _c.label = 1;
                    case 1:
                        _c.trys.push([1, 3, , 4]);
                        _b = (_a = JSON).parse;
                        return [4 /*yield*/, this.db.get(key)];
                    case 2:
                        openRevs = _b.apply(_a, [_c.sent()]);
                        return [3 /*break*/, 4];
                    case 3:
                        err_11 = _c.sent();
                        throw "not found";
                    case 4: return [2 /*return*/, openRevs];
                }
            });
        });
    };
    return StefDB;
}());
exports.StefDB = StefDB;
