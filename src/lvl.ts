import * as _leveldown  from 'leveldown';
import * as _levelup    from 'levelup';
import { StringDecoder } from 'string_decoder';

const leveldown = (<any>_leveldown).default || _leveldown
const levelup   = (<any>_levelup).default   || _levelup

interface KeyVal {
  key: any;
  value: any;
}

let db = levelup(leveldown('./tmp-db'));
const decoder = new StringDecoder('utf8');

db.createReadStream()
  .on('data', function (data: KeyVal) {
    console.log(decoder.write(data.key), '=', JSON.parse(data.value));
    console.log('-----');
  })
  .on('error', function (err: Error) {
    console.log('Error', err)
  })
  .on('close', function () {
  })
  .on('end', function () {
    console.log('done');
  })
