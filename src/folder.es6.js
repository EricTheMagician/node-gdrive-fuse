"use strict";

var fs = require( 'fs-extra');
var request = require( 'request');
var pth = require( 'path');
var mmm = require('mmmagic');

var common = require('./common.es6.js');
var config = common.config
var dataLocation = common.dataLocation;
var uploadLocation = common.uploadLocation;
var downloadLocation = common.downloadLocation;
var logger = common.logger;
var oauth2Client = common.oauth2Client;
var refreshToken = common.refreshAccessToken;


var Magic = mmm.Magic;
var magic = new Magic(mmm.MAGIC_MIME_TYPE);

var uploadTree = new Map();



/*
 ############################################
 ######### Upload Helper Functions ##########
 ############################################
 */
var uploadUrl = "https://www.googleapis.com/upload/drive/v2/files?uploadType=resumable";
var rangeRegex =  /^(bytes)\=(\d*)\-(\d*)$/;

// rangeRegex =  ///
//    ^(bytes)\=  #remove keyword bytes
//    (\d*)        #finds the start
//    \-         #finds the separator
//    (\d*)$       #finds the end
//   ///


function getRangeEnd(range){
  return parseInt(range.match(rangeRegex)[3]);
}

function getNewRangeEnd(location, fileSize, cb){
  var options = {
    url: location,
    method: 'POST',
    headers: {
      "Authorization": `Bearer ${config.accessToken.access_token}`,
      "Content-Length": 0,
      "Content-Range": `bytes */${fileSize}`
    }
  };
  request.post(options, function requestGetNewRangeEndCallback(err, resp, body){
    if( resp.statusCode == 308  ){
      const header = resp.headers;
      const range = resp.headers.range || resp.headers.Range;
      if(!range){ //sometimes, it doesn't return the range, so assume it is 0.
        // logger.error resp.headers
        // logger.error res
        cb(resp.statusCode, -1);
        return;
      }

      const end = getRangeEnd(range);
      setImmediate( function(){cb(null,end)});
      return;
    }

    // unhandle error
    logger.debug("unhandled error with getting a new range end", resp.statusCode);

    setImmediate( function(){cb(resp.statusCode, -1)});
    return;



  });
  /*
  rest.put(location, options)
      .on( 'complete', function getNewRangeEndCompleteCallback(res, resp){
        if(res instanceof Error){
          // logger.debug "there was a problem getting a new range end"
          // logger.debug "result", res
          // logger.debug "resp", resp
          refreshToken(
              function refreshTokenGetNewRangeEndCallback(){
                getNewRangeEnd(location, fileSize, cb);
              }
          );
          return;
        }else{

          // #if the link is dead or bad
          if( resp.statusCode == 404 || resp.statusCode == 410 || resp.statusCode == 401){
            // logger.debug "the link is no longer valid"
            cb(resp.statusCode, -1);
            return;
          }

          var range = resp.headers.range || resp.headers.Range;
          if(!range){ //sometimes, it doesn't return the range, so assume it is 0.
            // logger.error resp.headers
            // logger.error res
            cb(resp.statusCode, -1);
            return;
          }

          var end = getRangeEnd(range);
          cb(null,end)
        }

        return
      })
    */
  return;
}

function getUploadResumableLink(parentId, fileName, fileSize, mime, cb){
  var data = {
    "parents": [{"id": parentId}],
    "title": fileName
  };

  var options = {
    url: uploadUrl,
    method: 'POST',
    timeout: 300000,
    headers: {
      "Authorization": `Bearer ${config.accessToken.access_token}`,
      "X-Upload-Content-Type": mime,
      "X-Upload-Content-Length": fileSize
    },
    body: data,
    json: true
  };

  request(options, function getUploadResumableLinkCompleteCallback(err, resp, result){
    if( resp.statusCode == 401 || resp.statusCode == 400){
      if( parseInt(resp.headers['content-length']) > 0){
        // logger.error "There was an error with getting a new resumable link"
        // logger.error result
        if( result.error){
          var error = result.error.errors[0];
          var idx = error.message.indexOf("Media type")
          if( idx >= 0 ){
            cb("invalid mime");
            return;
          }
        }
      }else{
        // logger.debug result
      }

      // logger.debug "refreshing access token while getting resumable upload links"

      refreshToken(
          function getUploadResumableLinkCompleteRetry(){
            getUploadResumableLink(parentId, fileName, fileSize, mime, cb);
          }
      )
    }else if(resp.statusCode == 200){
      cb(null, resp.headers.location);
    }else{
      // # console.log resp.statusCode
      // console.log(resp.headers)
      // console.log(resp.req._headers)
      // console.log(result)
      cb(resp.statusCode);
    }
  });
  
  
  /* 
  rest.postJson( uploadUrl, data, options)
      .on('complete', function getUploadResumableLinkCompleteCallback(result, resp){
        if(result instanceof Error){
          // logger.debug "there was an error with getting a new upload link"
          // logger.debug "result", result
          // logger.debug "response", resp
          refreshToken(
              function getUploadResumableLinkCompleteRetry(){
                getUploadResumableLink(parentId, fileName, fileSize, mime, cb);
              }
          );
        }else{
          if( resp.statusCode == 401 || resp.statusCode == 400){
            if( parseInt(resp.headers['content-length']) > 0){
              // logger.error "There was an error with getting a new resumable link"
              // logger.error result
              if( result.error){
                var error = result.error.errors[0];
                var idx = error.message.indexOf("Media type")
                if( idx >= 0 ){
                  cb("invalid mime");
                  return;
                }
              }
            }else{
              // logger.debug result
            }

            // logger.debug "refreshing access token while getting resumable upload links"

            refreshToken(
                function getUploadResumableLinkCompleteRetry(){
                  getUploadResumableLink(parentId, fileName, fileSize, mime, cb);
                }
            )
          }else if(resp.statusCode == 200){
            cb(null, resp.headers.location);
          }else{
            // # console.log resp.statusCode
            // console.log(resp.headers)
            // console.log(resp.req._headers)
            // console.log(result)
            cb(resp.statusCode);
          }
        }
      });
  */
}

function uploadData(location, fileLocation, start, fileSize, mime, cb){

  // read the data
  var readStreamOptions = {
    start: start
  };

  var requestOptions = {
    method: "PUT",
    url: location,
    headers:{
      "content-type": mime,
      "Authorization": `Bearer ${config.accessToken.access_token}`,
      "Content-Length": (fileSize) - start,
      "Content-Range": `bytes ${start}-${fileSize-1}/${fileSize}`
    }
  };
  function uploadGetNewRangeEndCallback(err,end){
    setImmediate( 
      function(){cb(err, { rangeEnd: end});}
    );
  }
  function uploadRequestCallback(err, resp, body){


    if(err) {
      getNewRangeEnd(location, fileSize, uploadGetNewRangeEndCallback);
      return;
    }


    if(resp.statusCode == 400 || resp.statusCode == 401 || resp.statusCode == 410){

      getNewRangeEnd(location, fileSize,
          function uploadGetNewRangeEndCallbackAferError(err,end){
            logger.debug(end);
            cb( err, {
              statusCode: resp.statusCode,
              rangeEnd: end
            });
          }
      );

      return;
    }

    if(resp.statusCode == 404){
      cb( 404, JSON.parse(body) );
      return;
    }
    if(resp.statusCode == 308){ // success on resume
      var rangeEnd = getRangeEnd(resp.headers.range)
      cb( null, {
        statusCode: 308,
        rangeEnd: rangeEnd
      });
      return;
    }

    if ( 200 == resp.statusCode || resp.statusCode == 201){
      cb( null, {
        statusCode: 201,
        rangeEnd: fileSize,
        result: JSON.parse(body)
      });
      return
    }


    if (resp.statusCode >= 500){
      getNewRangeEnd(location, fileSize,
          function uploadGetNewRangeEndCallbackAferError500(err,end){
            cb( null, {
              statusCode: resp.statusCode,
              rangeEnd: end
            })
          }

      );
      return;
    }


    logger.error( "uncaugt state for file uploading" );
    logger.error( resp.statusCode );
    logger.error( resp.headers );
    logger.error( body     );

    getNewRangeEnd(location, fileSize,
        function newRangeEndErrorCallback(err,end){
          cb(err, {
            statusCode: resp.statusCode,
            rangeEnd: end
          });
        }
    );
  }

  var once = false;

  const rstream = fs.createReadStream( fileLocation, readStreamOptions);
  rstream.on('error', function(err){
    rstream.end();
  });

  const reqstream =       request(requestOptions, uploadRequestCallback);
  reqstream.on('error', function uploadErrorCallback(err){
    logger.error( "error after piping" );
    logger.error( err );
    reqstream.end();
    try{
      rstream.unpipe();
      rstream.pause();
    }catch(e){
      logger.error(e);
    }
    function uploadErrorCallbackGetNewRange(err,end){
      cb( err, {
        rangeEnd: end
      });
    }
    if(!once){
      once = true;
      setImmediate( function(){
        getNewRangeEnd(location, fileSize, uploadErrorCallbackGetNewRange);
      });
    }
  });


  rstream.pipe(
    reqstream
  );

}

var lockUploadTree = false;
function saveUploadTree(){
  if(!lockUploadTree){
    lockUploadTree = true;
    var toSave = {}
    for(let item of uploadTree){
      let key = item[0];
      let value = item[1];
      toSave[key] = value;
    }
    logger.debug("saving upload tree");
    fs.outputJson( pth.join(config.cacheLocation, 'data','uploadTree.json'), toSave, function unlockSavingUploadTree(err){
      if(err){
        logger.error("There was an error saving the upload tree");
        logger.error(err);
      }
      lockUploadTree = false;
    });
  }
}




// ######################################
// ######################################
// ######################################
class GFolder {
  constructor(id, parentid, name, ctime, mtime, permission, children, mode) {
    if (!children)
      children = [];
    if (!mode) {
      mode = 16895;//0o40777;
    }
    this.id = id;
    this.parentid = parentid;
    this.name = name;
    this.ctime = ctime; 
    this.mtime = mtime;
    this.permission = permission;
    this.children = children;
    this.mode = mode;
  }

  getAttrSync() {
    var attr = {
      mode: this.mode,
      size: 4096, //standard size of a directory
      nlink: this.children.length + 1,
      mtime: parseInt(this.mtime/1000),
      ctime: parseInt(this.ctime/1000),
      inode: this.inode
    };
    return attr;
  }

  getAttr(cb) {
    var attr = {
      mode: this.mode,
      size: 4096, //standard size of a directory
      nlink: this.children.length + 1,
      mtime: parseInt(this.mtime/1000),
      ctime: parseInt(this.ctime/1000),
      inode: this.inode
    };
    cb(0, attr);

  }


  upload(fileName, inode, cb) {
    const folder = this;
    const upFile = uploadTree.get(inode);
    if (!upFile) {
      cb({code: "ENOENT"});
      return;
    }
    const  filePath = pth.join(uploadLocation, upFile.cache);
    // if the file is already being uploaded, don't try again.
    if ( upFile.uploading) {
      logger.debug(`${fileName} is already being uploaded`);
      cb("uploading");
      return
    }
    upFile.uploading = true;


    fs.stat(filePath, function uploadStatCallback(err, stats) {
      if (err || stats == undefined) {
        logger.debug(`there was an errror while trying to upload file ${fileName}`);
        logger.debug(err);
        if (err.code == "ENOENT") {
          // file was deleted
          uploadTree.delete(inode);
        }
        upFile.uploading = false;
        cb(err);
        return;
      }

      const size = stats.size;

      // sometimes, the operating system will create a file of size 0. Simply delete it.

      if (size == 0) {
        fs.unlink(filePath, function deleteFilesCallback(err) {
          if (err) {
            logger.debug(`there was an error removing a file of size 0, ${filePath}`);
            logger.debug(err);
          }
          cb({code: "ENOENT"});
          upFile.uploading = false;
        });
        return;
      }

      function uploadFunction() {
        fs.stat(filePath, function uploadStatCallback2(err, stats2) {
          if (err || stats2 == undefined) {
            logger.debug(`there was an errror while trying to upload file ${fileName} with path ${inode}`);
            if (err.code == "ENOENT") {
              // file was deleted
              uploadTree.delete(inode);
            }
            cb(err)
            upFile.uploading = false;

            return;
          }

          if (size != stats2.size) { // make sure that the cache file is not being written to. mv will create, close and reopen
            setTimeout(
                function timeoutUploadCallback() {
                  upFile.uploading = false;
                  folder.upload(fileName, inode, cb);
                }
                , 10000);
            return;
          }
          upFile.uploading = true;
          magic.detectFile(filePath, function magicDetectFilesCallback(err, mime) {
            if (err) {
              logger.debug("There was an error with detecting mime type");
              logger.debug(err);
            }

            // if the mime type is binary, set it to application/octect stream so google will accept it
            if (mime === 'binary') {
              mime = 'application/octet-stream'
            }

            function cbUploadData(err, res) {
              if (err) {
                logger.error("There was an error with uploading data");
                logger.error(err);
                logger.error(res);
                getNewRangeEnd(upFile.location, size,
                    function getNewRangeFromUploadCallback(err, end) {
                      logger.debug("after failed upload");
                      logger.debug("error");
                      logger.debug(err);
                      logger.debug("end", end);
                      var up = uploadTree.get(inode);
                      if (!up) {
                        cb("ENOENT");
                        return;
                      }
                      up.uploading = false;
                      delete up.location;
                      folder.upload(fileName, inode, cb);
                      return;
                    }
                );
                return;
              } else {
                var start = res.rangeEnd + 1
                if (start < size) {
                  uploadData(upFile.location, filePath, start, size, mime, cbUploadData);
                } else {
                  logger.debug(`successfully uploaded file ${inode}`);
                  cb(null, res.result);
                }
              }
            }


            function cbNewLink(err, location) {
              if (err) {
                cb(err);
                return;
              }

              upFile.location = location;
              uploadTree.set(inode, upFile);
              saveUploadTree();

              //once new link is obtained, start uploading
              uploadData(location, filePath, 0, size, mime, cbUploadData);
            }

            function cbNewEnd(err, end) {
              if (err) {
                delete upFile.location
                logger.debug(`there was an error with getting a new range end for ${inode}`);
                logger.debug("err", err);
                getUploadResumableLink(folder.id, fileName, size, mime, cbNewLink);
                return;
              }

              if (end <= 0) {
                logger.debug(`tried to get new range for ${inode}, but it was ${end}`);
                delete upFile.location;
                getUploadResumableLink(folder.id, fileName, size, mime, cbNewLink);
              } else {
                var start = end + 1;
                logger.debug(`got new range end for ${inode}: ${end}`);
                // once new range end is obtained, start uploading in chunks
                uploadData(upFile.location, filePath, start, size, mime, cbUploadData);
              }
            }

            logger.info( `Starting to upload file ${fileName} ` );
            if (upFile.location) {
              // const location = upFile.location;
              getNewRangeEnd(upFile.location, size, cbNewEnd);
            } else {
              getUploadResumableLink(folder.id, fileName, size, mime, cbNewLink);
            }
            return;

          });
        });
      }
      setTimeout(uploadFunction, 5000)
      
    });
  }
}



//load upload Tree
if( fs.existsSync(pth.join(config.cacheLocation, 'data','uploadTree.json')) ){
  logger.info( "loading upload tree" );
  fs.readJson( pth.join(config.cacheLocation, 'data','uploadTree.json'), function readUploadTreeCallback(err, data){
    try{
      for( let key of Object.keys(data) ){
        let value = data[key];
        value.uploading = false;
        uploadTree.set( parseInt(key), value)
      }
    }catch (error){
      logger.error("There was an error parsing upload tree");
      logger.error(error);
    }
  });
}


module.exports.GFolder = GFolder
module.exports.uploadTree = uploadTree
module.exports.saveUploadTree = saveUploadTree

