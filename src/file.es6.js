"use strict";
const pth = require('path');
const fs = require('fs-extra');
const winston = require('winston');
const EventEmitter = require('events').EventEmitter;
const request = require('request');

const common = require('./common.es6.js');
const config = common.config
const dataLocation = common.dataLocation;
const uploadLocation = common.uploadLocation;
const downloadLocation = common.downloadLocation;
const logger = common.logger;
const GDrive = common.GDrive;
const oauth2Client = common.oauth2Client;
const refreshAccessToken = common.refreshAccessToken
const maxCache = common.maxCache;

const queue = require('queue');
const db = common.database;
const q = queue({concurrency: 1, timeout: 7200000 });
let totalDownloadSize = 0;
const regexPattern = /^[a-zA-Z0-9-]*-([0-9]*)-([0-9]*)$/;

const MD5 = require( 'MD5' );


// opened files
const openedFiles = new Map();
const downloadTree = new Map();
const buf0 = new Buffer(0);

/*
######################################
######### Create File Class ##########
######################################
*/

const baseUrlForDownload = "https://www.googleapis.com/drive/v2/files/"

class GFile extends EventEmitter{

  // static get chunkSize(){
  //   return 1024*1024*16; //set default chunk size to 16MB. this should be changed at run time
  // }
  constructor(downloadUrl, id, parentid, name, size, ctime, mtime, permission, mode){
    super();
    if(!mode){
      mode = 33279;//0o100777;
    }
    this.downloadUrl = downloadUrl;
    this.id = id;
    this.parentid = parentid;
    this.name = name;
    this.size = size;
    this.ctime = ctime;
    this.mtime = mtime;
    this.permission = permission;
    this.mode = mode;
  }

  __download(start,end, cb ){

    /* private function where the actual downloading is done */
    const file = this;
    const url = file.downloadUrl;
    const saveLocation = pth.join(downloadLocation, `${file.id}-${start}-${end}`);
    const size = file.size;

    if(config.accessToken == null){
      logger.debug("access token was null when downloading files");
      cb("expiredUrl");
      return;
    }

    const options ={
      url: file.downloadUrl,
      // url: `${baseUrlForDownload}${file.id}?alt=media`,
      encoding: null,
      headers:{
        "Authorization": `Bearer ${oauth2Client.credentials.access_token}`,

        "Range": `bytes=${start}-${end}`,
      }
    };

    try{
      request(options, function downloadRequestCallback(err, resp, body ){
        if(err){
          logger.error("There was an error with downloading");
          logger.error(err);
          cb(err);
        }

        // make sure the body is a buffer
        if(Buffer.isBuffer(body)){

          //make sure the buffer is the right size
          if(body.length == (end-start+1)){
            fs.writeFile(saveLocation,body, function writeFileCallback(err, bytesWritten){
              let base = pth.basename(saveLocation);
              addNewFile(base,'downloading', body.length );
              cb(null);
            });

            return;
          }else {

            try{
          
              if( body.length == 0 && resp.statusCode >= 400 && resp.statusCode < 500){
                cb("expiredUrl");
                return;
              }

              const error = JSON.parse(body).error;
              if( error.code == 401 ){
                cb(error.message);
                return;
              } 

            }catch(e){
              cb(err);
            }

          }
        } 


      });
    }catch(e){
        logger.error("There was an uncaught error while downloading" );
        logger.error( e );      
    }

    // const ws = fs.createWriteStream(saveLocation);
    // ws.on('error', function downloadWriteStreamError(err){
    //   logger.error("There was an error with writing during the download");
    //   logger.error(err);
    //   if (err.code == "EMFILE"){
    //     logger.debug( "There was an error with downloading files: EMFILE" );
    //     logger.debug( err );
    //   }
    //   cb(err);
    //   this.end();
    //   return;
    // });
    // var once = false;
    // try{
    //   request(options)
    //   .on( 'response', function requestResponseCallback(resp){
    //     if(resp.statusCode === 401 || resp.statusCode === 403 ){
    //       if (!once){
    //         once = true;
    //         setTimeout(
    //             function expiredUrlTimeout(){
    //               cb("expiredUrl");
    //             }
    //             , 2000);
    //       }
    //       ws.end();
    //       this.end();
    //       return;
    //     }
    //     if(resp.statusCode >= 500){
    //       if(!once){
    //         once = true;
    //         setTimeout(
    //             function requestResponseCallback500(){
    //               cb(500);
    //             }
    //             , 1000);
    //       }
    //       ws.end();
    //       this.end();
    //       return;
    //     }
    //   })
    //   .on( 'error', function writeStreamErrorCallback(err){
    //     if (!once){
    //       once = true;
    //       logger.error( "error" );
    //       logger.error( err );
    //       logger.error( err.code );
    //       if (err.code == "EMFILE"){
    //         logger.debug( "There was an error with downloading files: EMFILE" );
    //         logger.debug( err );
    //       }

    //       cb(err);
    //     }
    //     this.end();
    //     ws.end();
    //   })
    //   .pipe(ws)
    //   .on('error', function pipeDownloadStreamErrorCallback(err){
    //     logger.error("There was an error with piping during the download");
    //     logger.error(err);
    //     if (err.code == "EMFILE"){
    //       logger.debug("There was an error with downloading files: EMFILE");
    //       logger.debug(err);
    //     }
    //     if(!once){
    //       once = true;
    //       cb(err);
    //     }
    //     this.end();
    //     ws.end();
    //   })
    //   .on('close', function pipeStreamCloseCallback(){
    //     if(!once){
    //       once = true
    //       let base = pth.basename(saveLocation);
    //       let chunkSize = end-start + 1;
    //       addNewFile(base,'downloading', chunkSize )
    //       cb(null)
    //     }
    //     this.end();
    //     ws.end()
    //   })
    //   return;
    // }catch(e){
    //   logger.error("There was an uncaught error while downloading" );
    //   logger.error( e );
    //   ws.end();
    // }
    return;
  }
  getAttrSync(){
    const attr = {
      mode: this.mode,
      size: this.size,
      nlink: 1,
      mtime: parseInt(this.mtime/1000),
      ctime: parseInt(this.ctime/1000),
      inode: this.inode
    }
    return attr;
  }

  getAttr(cb){
    const attr = {
      mode: this.mode,
      size: this.size,
      nlink: 1,
      mtime: parseInt(this.mtime/1000),
      ctime: parseInt(this.ctime/1000),
      inode: this.inode
    };
    cb(0,attr);
  }

  recursive(start,end){
    const file = this;
    const path = pth.join(downloadLocation, `${file.id}-${start}-${end}`);
    if(start >= file.size){
      return;
    }
    file.open(start, function openFileCallbackRecursive(err, fd){
      if(err || fd == false){
        if (!downloadTree.has(`${file.id}-${start}`)){
          logger.silly(`starting to recurse ${file.name}-${start}`);
          downloadTree.set(`${file.id}-${start}`, 1);
          file.__download(start,end, function recursiveDownloadCallback(err){
            if(err){
              if( ! (err === "expiredUrl" || err === "Invalid Credentials") ){
                logger.error(`There was an error while during recursiveDownloadCallback`);
                logger.error(err);
              }
              // file.__download(start,end,recursiveDownloadCallback);
            }
            downloadTree.delete(`${file.id}-${start}`);
            file.emit( 'downloaded', start);
            function emitFromRecursiveTimeout(){
              file.emit( 'downloaded', start);
            }
            setTimeout( emitFromRecursiveTimeout, 1000);
            logger.silly(`finishing recurse ${file.id}-${start}`);
          });
        }
      }
    });
  }

  open(_start,cb){
    let start = _start;
    let file = this;
    function openedFileCallCloseTimeout(){
      let opened = openedFiles.get(`${file.id}-${start}`);
      if(opened){
        if(!opened.fd ){
          // debugger;
          logger.debug( "opened.fd was false");
          logger.debug( file);
          logger.debug( opened);
          return ;
        }
        fs.close(opened.fd, function openedFileCallCloseCallback(err){
          if (err){
            logger.error( `There was an error with closing file ${file.name}-${start} with fd ${opened.fd}` );
            logger.error( err );
          }
          openedFiles.delete(`${file.id}-${start}`);
        });
      }
    }
    
    const cacheTimeout = 6000 ;
    if(openedFiles.has( `${file.id}-${start}`)){
      let f = openedFiles.get(`${file.id}-${start}`);
      clearTimeout(f.to)
      f.to = setTimeout(openedFileCallCloseTimeout, cacheTimeout);
      cb(null, f.fd);
      openedFiles.set(`${file.id}-${start}`, f);
      return;
    }else{
      let end = Math.min(start + config.chunkSize, file.size ) - 1;
      let path = pth.join(downloadLocation, `${file.id}-${start}-${end}`);
      try{
        fs.stat( path, function open_file_stat_callback(err, stats){
          if (err){
            logger.silly( "there was an error stat-ing a file in file.open" );
            logger.silly( err );
            cb(err,false)
            return
          }
          if( stats.size == (end - start + 1)){
            fs.open( path, 'r', function openfile_callback(err,fd){
              if (err){
                if (err.code == "EMFILE"){
                  file.open(start, cb);
                }else{
                  logger.error("there was an handled error while opening files for reading");
                  logger.error(err);
                  cb(err);
                }
                return;
              }
              
              // make sure that there's only one file opened.
              // multiple files can be opened at once because of the fuse multithread
              if(openedFiles.has(`${file.id}-${start}`)){
                let opened = openedFiles.get(`${file.id}-${start}`);
                clearTimeout(opened.to)

                cb(null, opened.fd)
                fs.close(fd, function close_already_opened_file_callback(err){
                  if (err){
                    logger.error( "There was an error closing an already opened file" )
                    logger.error( err )
                  }
                  return;
                });

                opened.to = setTimeout(openedFileCallCloseTimeout, cacheTimeout)
                openedFiles.set(`${file.id}-${start}`, opened);

                return;
              }

              openedFiles.set(`${file.id}-${start}`, {fd: fd, to: setTimeout(openedFileCallCloseTimeout, cacheTimeout) });
              cb(null, fd);
              return;
            });
          }else{
            cb(null, false);
          }
          return;
        });
      }catch(e){
        cb(null, false)
      }
    }
  }




  read(start,end, readAhead, cb){
    let file = this;
    end = Math.min(end, this.size-1);
    let chunkStart = Math.floor((start)/config.chunkSize)* config.chunkSize;
    let chunkEnd = Math.min( Math.ceil(end/config.chunkSize) * config.chunkSize, file.size)-1;
    let nChunks = (chunkEnd - chunkStart)/config.chunkSize;
    function _readAheadFn(){
      if (readAhead){
        if (chunkStart <= start < (chunkStart + 131072)){
          file.recursive( Math.floor(file.size / config.chunkSize) * config.chunkSize, file.size-1);
          for(let i = 1; i<= config.advancedChunks; i++ ){
            file.recursive(chunkStart + i * config.chunkSize, chunkEnd + i * config.chunkSize);
          }
        }
      }
    }
    let __once__ = false;
    function listenCallback(cStart){
      if (!__once__){
        // #logger.silly "listen callback #{file.id}-#{chunkStart},#{cStart}"
        if ( cStart <= start < (cStart + config.chunkSize-1)  ){
          // #logger.debug "once #{ __once__ } -- #{cStart} -- #{start}"
          __once__ = true;
          file.removeListener( 'downloaded', listenCallback);
          // #logger.silly "listen callback #{file.id}-#{chunkStart}"
  
          // # we need to re-emit because of the -mt flag from fuse.
          // # otherwise, this
          file.emit('downloaded', cStart);
          file.read(start,end, readAhead, cb);
        }
      }
    }

    if(nChunks < 1){

      // var path = pth.join(downloadLocation, `${file.id}-${chunkStart}-${chunkEnd}`);
      if (downloadTree.has( `${file.id}-${chunkStart}` ))
      {
        logger.silly(`download tree has ${file.id}-${chunkStart}`);
        file.on('downloaded', listenCallback);
        _readAheadFn();
        return;
      }
  
      // try to open the file or get the file descriptor
      file.open( chunkStart, function openFileFromRead(err,fd){
          
        //fd can returns false if the file does not exist yet
        if(err || fd == false){
          file.download(start, end, readAhead, cb);
          _readAheadFn();
          return;
        }

        //if the file is already opened
        downloadTree.delete( `${file.id}-${chunkStart}`);

        // if the file is opened, read from it
        const readSize = end-start;
        const buffer = new Buffer(readSize+1);
        try{
          fs.read( fd,buffer, 0, readSize+1, start-chunkStart, function fsReadCallback(err, bytesRead, buffer){
            if(err){
              logger.error(`There was an error while reading file -- ${file.name} -- ${file.id}-${start}`);
            }
            cb(buffer.slice(0,bytesRead));
          });
          _readAheadFn();
        }catch(e){
          logger.error( "There was an error while reading file. Retrying");
          logger.error( e);
          file.read(start,end, readAhead, cb);
        }

      });
  

    }else if(nChunks < 2){
      const end1 = chunkStart + config.chunkSize - 1;
      const start2 = chunkStart + config.chunkSize;

      file.read( start, end1,true, function callback1_multiple_chunks(buffer1){
        if(buffer1.length == 0){
          cb(buffer1);
          return;
        }
        function callback2_multiple_chunks(buffer2){
          if(buffer2.length == 0){
            cb(buffer1);
            return;
          }
          cb( Buffer.concat([buffer1, buffer2]) );
        }

        file.read( start2, end, true, callback2_multiple_chunks);
      });

    }else{
      logger.debug(`too many chunks requested, ${nChunks}`);
      cb(buf0);
    }

  }

  updateUrl(cb){
    const file = this;
    const data = {
      fileId: file.id,
      acknowledgeAbuse  : true,
      fields: "downloadUrl"
    };
    GDrive.files.get( data, function updateUrlCallback(err, res){
      if (err){
        if(err.code == 404){
          logger.error(`The file "${file.name}" could not be found on Google's server. It's likely to have been deleted.`)
          cb(null);
          return;
        }
        logger.error( `There was an error while getting an updated url for ${file.name}` );
        logger.error( err );
        file.updateUrl(cb);
        return;
      }
      file.downloadUrl = res.downloadUrl;

      refreshAccessToken( function refreshAccessTokenFromUpdateUrlCallback(){
        cb(file.downloadUrl);
      });
    });
  }

  download(start, end, readAhead, cb){
    // if file chunk already exists, just download it
    // else download it
    const file = this;
    const chunkStart = Math.floor((start)/config.chunkSize) * config.chunkSize;
    const chunkEnd = Math.min( Math.ceil(end/config.chunkSize) * config.chunkSize, file.size)-1; //and make sure that it's not bigger than the actual file
    const nChunks = (chunkEnd - chunkStart)/config.chunkSize;

    function downloadSingleChunkCallback(err){      

      function retryDownloadChunkOnErr(url){
        if(url){
          file.__download(chunkStart, chunkEnd, downloadSingleChunkCallback);
        }else{
          downloadTree.delete(`${file.id}-${chunkStart}`)
          file.emit("downloaded", chunkStart);
          cb(null)
        }
      }

      function emitDownloadCallbackTimeout(){
        file.emit('downloaded', chunkStart);
      }
      if (err){
        if (err === "expiredUrl"){
          file.updateUrl( retryDownloadChunkOnErr );          
        } else if (err === "Invalid Credentials"){
          common.refreshAccessToken(retryDownloadChunkOnErr);
        }else{
          logger.error( "there was an error downloading file");
          logger.error( err);
          cb(buf0);
          downloadTree.delete(`${file.id}-${chunkStart}`);
          file.emit('downloaded', chunkStart);
          setTimeout(emitDownloadCallbackTimeout, 1000);
        }
        return;
      }
      downloadTree.delete(`${file.id}-${chunkStart}`);
      file.read(start,end, readAhead, cb);
      file.emit('downloaded', chunkStart);
    }
    if( nChunks < 1){
      if (downloadTree.has(`${file.id}-${chunkStart}`)){
        file.read(start,end,readAhead,cb);
      }else{
        logger.debug( `starting to download ${file.name}, chunkStart: ${chunkStart}` );
        downloadTree.set(`${file.id}-${chunkStart}`, 1);
        file.__download(chunkStart, chunkEnd, downloadSingleChunkCallback);
      }

    }else if(nChunks < 2){
      const end1 = chunkStart + config.chunkSize - 1;
      const start2 = chunkStart + config.chunkSize;
      file.read( start, end1,true, function callback1_downloading_multiple_chunks(buffer1){
        function callback2_downloading_multiple_chunks(buffer2){
          if( buffer2.length == 0){
            cb(buffer1);
            return;
          }
          cb( Buffer.concat([buffer1, buffer2]) );
        }
        if (buffer1.length == 0){
          cb(buffer1);
          return;
        }

        file.read( start2, end, true, callback2_downloading_multiple_chunks);
        return;
      });
    }else{
      logger.debug(`too many chunks requested, ${nChunks}`);
      cb(buf0);
    }

  }

  getCacheName(){
    return MD5(this.parentid + this.name);
  }
}

/*
* 
* Download folder watcher:  prevent download folder from getting too big
*
*/

function queue_fn(size, cmd){
  function queuing_function_callback(done){
    db.run( cmd, function queuing_function_insert_callback(err){
      if (err){
        logger.error( `init run path - ${cmd}` );
        logger.error( err );
        done();
        return;
      }

      totalDownloadSize += size
      if (totalDownloadSize > 0.90*maxCache){
        delete_files()
      }
      logger.silly( `totalDownloadSize: ${totalDownloadSize}` )
      done()
      return
    });
  }
  return queuing_function_callback;
}
function initialize_path(path, type){
  fs.readdir( path, function initialize_path_callback(err, files){
    if(err){
      logger.error("There was an error while initializing the file cache database");
      logger.error(err);
      return;
    }
    var count = 0
    var totalSize = 0
    const basecmd = "INSERT OR IGNORE INTO files (name, atime, type, size) VALUES "
    var cmd = basecmd
    for( let file of files){
      const expectedSize = file.match(regexPattern)
      if(expectedSize != null){
        const size = Math.max(parseInt(expectedSize[2])- parseInt(expectedSize[1]) + 1, 0)
        if(size == 0){
          logger.debug( `expectedSize for ${file} is 0. ${expectedSize}` )
        }
        cmd += `('${file}', 0, '${type}', ${size})`
        count += 1
        totalSize += size

        if(count > 25000){
          q.push( queue_fn(totalSize, cmd))
          count = 0
          totalSize = 0
          cmd = basecmd
        }else{
          cmd += ','
        }
      }else{
        logger.debug( `expectedSize is null for this file: ${file}`)
      }
    }



    // Make sure the queue is empty
    if (count > 0){
      q.push( queue_fn(totalSize, cmd.slice(0,-1))) ; //remove the last comma
      count = 0;
      totalSize = 0;
      cmd = basecmd;
    }

    q.start();
  });

}
var delete_once = false
function delete_files(){
  if( !delete_once){
    delete_once = true;
    logger.info( "deleting files to make space in the cache" );
    logger.info( `current size of cache is: ${totalDownloadSize/1024/1024/1024} GB` );

    db.all("SELECT * from files ORDER BY atime, size ASC", function delete_files_callback(err, rows){
      _delete_files_(0,0,rows);
    });
  }
}

function _delete_files_(start,end, rows){
  var row = rows[end]
  var count = end - start + 1
  if( totalDownloadSize >= (0.8*maxCache) ){
    fs.unlink( pth.join(downloadLocation, row.name), function unlink_delete_file_callback(err){
      if(!err){
        // if there is an error, it usually is because there was a file that was in the db that was already deleted
        totalDownloadSize -= row.size;
      }

      if (count > 2000){
        let cmd = "DELETE FROM files WHERE name in (";
        for( var i = start; i < end; i++){
          row = rows[i];
          cmd += `'${row.name}',`;
        }
        cmd += `'${rows[end].name}')`;

        db.run( cmd, function remove_rows_from_database_after_delete(err){
          if (err){
            logger.error( "There was an error with database while deleting files" )
            logger.err( err )
            logger.info( "finsihed deleting files by error" )
            logger.info( `current size of cache is: ${totalDownloadSize/1024/1024/1024} GB` )
            delete_once = false
            return
          }

          end += 1;
          if (end == rows.length){
            logger.info( "finsihed deleting files by delelting all files")
            logger.info( `current size of cache is: ${totalDownloadSize/1024/1024/1024} GB`)
            delete_once = false
          }else{
            _delete_files_(end, end, rows);
          }
        });


      }else{
        end += 1
        if (end == rows.length){
          logger.info( "finsihed deleting files by delelting all files");
          logger.debug( "and then running the database cmd");
          logger.info( `current size of cache is: ${totalDownloadSize/1024/1024/1024} GB`);
          delete_once = false;
        }else{
          _delete_files_(start, end, rows)
        }
      }
    });
  }else{
    if (end > start){
      var cmd = "DELETE FROM files WHERE name in ("
        for (var i = start; i < end; i++){
          row = rows[i];
          cmd += `'${row.name}',`;
        }
        cmd += `'${rows[end].name}')`;

      db.run( cmd, function remove_rows_from_database_after_delete(err){
        if (err){
          logger.error( "There was an error with database while final deleting files")
          logger.error( err)
        }
      });
      }
    logger.info( "finished deleting files" )
    logger.info( `current size of cache is: ${totalDownloadSize/1024/1024/1024} GB` )
    delete_once = false
  }

}



function addNewFile(file, type, size){
  // db.run "INSERT OR REPLACE INTO files (name, atime, type, size) VALUES ('#{file}', #{Date.now()}, '#{type}', #{size})", ->
  //   totalDownloadSize += size
  //   console.log totalDownloadSize
  var cmd = `INSERT OR REPLACE INTO files (name, atime, type, size) VALUES ('${file}', ${Date.now()}, '${type}', ${size})`
  q.push(queue_fn(size,cmd));
  q.start();
}

db.run(  "CREATE TABLE IF NOT EXISTS files (size INT, name TEXT unique, type INT, atime INT)", function init_database_callback(err){
  if (err){
    logger.log (err)
  }

  logger.info( "Opened a connection to the database" );
  // initialize_db()
  initialize_path( downloadLocation, "downloading");
});

module.exports.GFile = GFile
module.exports.addNewFile = addNewFile
module.exports.queue_fn = queue_fn
