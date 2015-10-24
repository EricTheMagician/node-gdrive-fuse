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
const commonStatus = common.commonStatus;
const f = require("./file.es6.js");

const GFile = f.GFile;
const addNewFile = f.addNewFile;
const queue_fn = f.queue_fn;

const folder = require("./folder.es6.js");
const GFolder = folder.GFolder;

const inodeTree = require('./inodetree.js')

const queue = require('queue');
const uploadQueue = queue({concurrency: config.maxConcurrentUploads || 4, timeout: 14400000 }); // default to 4 concurrent uploads

const uploadTree = new Map();

const mmm = require('mmmagic');
const Magic = mmm.Magic;
const magic = new Magic(mmm.MAGIC_MIME_TYPE);


/*
 ############################################
 ######### Upload Helper Functions ##########
 ############################################
 */
const rangeRegex =  /^(bytes)\=(\d*)\-(\d*)$/;

// rangeRegex =  ///
//    ^(bytes)\=  #remove keyword bytes
//    (\d*)        #finds the start
//    \-         #finds the separator
//    (\d*)$       #finds the end
//   ///

function getRangeEnd(range){
  return parseInt(range.match(rangeRegex)[3]);
}

var lockUploadTree = false;
function saveUploadTree(){
  if(!lockUploadTree){
    lockUploadTree = true;
    var toSave = {}
    for(let item of uploadTree){
      let key = item[0];
      let value = item[1];
      toSave[key] = value.toJson();
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

class UploadingFile {
	constructor(inode, filename, parentid, released, callback){
		
		/*
		
		released: boolean. if set, the underlying filesystem has released the file.
		
		temporary files will be stored in the uploading location in the following way:
		uploadLocation/parentid/filename
		
		*/
		this.inode = inode; // associated inode
		this.file = inodeTree.getFromInode(inode); //assoicated GFile
		this.filename = filename; //filename
		this.parentid = parentid; //parent id
		this.uploading = false; //status of uploading. only it to be uploaded once. just in case it is uploading twice. 

		if(released){
			this.released = true;
		}else{
			this.released = false;
		}
		
		this.newName = filename; // if the file is being uploaded, but it needs to be renamed, finish uploading and then rename it.
		this.newParent = parentid; // if the file is being uploaded, but it needs to be moved, finish uploading and then move it.
		this.uploadUrl = null; // the url to use for the upload
		this.toBeDeleted = false; // if the file is being uploaded and then the filesystem reuqests a deletion, finish uploading it and then delete it.
    	this.mime = null; // the mime type of the file being uploaded

		// ensure that the parentid folder exists in the upload location
		const uploadFolder = pth.join(uploadLocation, parentid);
		const self = this;
		this.uploadedFileLocation = pth.join(uploadFolder,filename);
		fs.ensureDir(uploadFolder, function (err){
			if(callback != null && typeof(callback) == 'function'){
				setImmediate(callback,err,self.uploadedFileLocation);	
			}			
		});
		
		saveUploadTree();
					
	}		
	
	static resumeUploadingFilesFromUploadFolder(callback){
		/*	
		sometimes, the uploadTree.json file will get corrupted.
		as a safeguard, read the files list from the upload data folder and try to find 
		the associated inode and restart the upload.
		*/
	
		fs.readdir(uploadLocation, function (err, folders){
			
			/* 
			everything in the upload location should be a parentid for a folder			
			*/
						
			for(let folder of folders){
				
				let parent = inodeTree.getFromId(folder);
				if(!parent){continue;}

				/* 
				scan the folder for files
				*/

				fs.readdir( pth.join(uploadLocation, folder), function(err, files){
					if( files.length > 0){
						for( let fileName of files ){
							let file = inodeTree.findChildByNameFromParentId(parent.id,fileName);
							if( file == null ){
								/* 
								if there is no reference to this file, then it should be deleted;
								*/
								fs.unlink(pth.join(uploadLocation,folder,fileName), function(err){
									/* 
									if the folder is empty now, it should be deleted. however,
									it is simpler to let it be deleted at the next startup if it is still empty 
									*/									
								});
										
							}else{								

								/* 
								make sure that the file that is found is not already in the upload tree
								*/
								if( !uploadTree.has(file.inode)){
									const upFile = new UploadingFile(file.inode, fileName, parent.id, true, null);
									uploadTree.set(file.inode, upFile);
									uploadQueue.push( upFile.upload.bind(upFile) );		
									uploadQueue.start();
								}

							}
						}
					}
				});
				
			}

	
		});		

	}

	
	setReleased(){
		this.released = true;
	}
	
	setUploadUrl(uploadUrl, callback){
		
		const self = this;
		/* if uploadUrl is valid, ensure that the underlying file exists */		
		if(uploadUrl && typeof(uploadUrl)=='string'){
			fs.stat(self.uploadedFileLocation, function(err, stat){				
				if(callback != null && typeof(callback) == 'function'){
					setImmediate(callback,err, stat);	
				}			
			})
		}
	}
	postUploadRenaming(){
		/* only rename if the parent or the filename is different from the new one */
		if(this.newName != this.filename || this.parentid != this.newParent){
			const self = this;
			const file = this.file;
			const params = {
				resource:{
					title: this.newName
				},
				fileId: this.file.id,
				modifiedDate: true
			};
			

			if( file.parentid != this.newParent ){
				params.addParents = this.newParent;
				params.removeParents =  this.parentid;
			}
			
			GDrive.files.patch( params, function filesPatchCallback(err){
				if(err){
					logger.error(`There was an error with renaming file (${file.name}) after it was finished uploading`);
					logger.debug(self);
					logger.debug(file);
					setImmediate( self.postUploadRenaming.bind(self) );
				}
			});
			
		}
	}
	postUploadDeleting(){
		if(this.toBeDeleted){
			const self = this;
			const file = self.file;
			GDrive.files.trash( {fileId: file.id}, function deleteFileCallback(err, res){
				if (err){
					logger.error( `after uploading: unable to remove file ${file.name} with id ${file.id}` );
				}
			});                

		}
	}
	postUploadProcessing(fd, start,cb){
		const self = this;
		const file = self.file;
		const uploadedFileLocation = self.uploadedFileLocation;
		const end = Math.min(start + config.chunkSize, file.size)-1;
		const savePath = pth.join(config.cacheLocation, 'download', `${file.id}-${start}-${end}`);
		const rstream = fs.createReadStream(uploadedFileLocation, {fd: fd, autoClose: false, start: start, end: end})
		const wstream = fs.createWriteStream(savePath);
	
		rstream.on('end',  function moveToDownloadReadStream(){
			/* Recursively move the uploading file until it is finished */			
			start += config.chunkSize;
			wstream.end();
			if (start < file.size){
				self.postUploadProcessing(fd, start, cb);
				return;
			}
			
			/* Once the uploading file is done, check for renaming */
			self.postUploadRenaming();
			self.postUploadDeleting();
			/* Once the uploading file is moved, close it and add it to the database */

			fs.close( fd, function moveToDownloadFinishCopying(err){
				if(err){
					logger.error( `There was an error closing file ${fd} - ${file.id} - ${file.name} after moving upload file to download` );
					logger.error( err );
				}
				var start = 0
				var end = Math.min(start + config.chunkSize, file.size)-1
				var totalSize = 0
				var count = 0
				const basecmd = "INSERT OR REPLACE INTO files (name, atime, type, size) VALUES "
				var cmd = basecmd
				while(start < file.size){
					var size = end - start + 1
					count += 1
					totalSize += size
					if(count > 750){
						cmd += `('${file.id}-${start}-${end}',${Date.now()},'downloading',${size})`
						queue_fn(totalSize, cmd)(function(){});
						cmd = basecmd;
						count = 0;
						totalSize = 0;
					}else{
						cmd += `('${file.id}-${start}-${end}',${Date.now()},'downloading',${size}),`
					}
					start += config.chunkSize;
					end = Math.min(start + config.chunkSize, file.size)-1
				}
				queue_fn(totalSize,cmd.slice(0,-1))(function(){});
				if (err){
					logger.debug(`unable to close file after transffering ${self.filename}`);
					setImmediate(cb,err);
					return;
				}
				fs.unlink( self.uploadedFileLocation, function deleteUploadedFile(err){
					if (err){
						logger.error( `unable to remove file ${self.filename}`);
					}
					setImmediate(cb);
				});
			});
		});
	
		rstream.pipe(wstream);
	}
	
	getUploadResumableLink(callback){

		const upFile = this;
		const file = upFile.file;
		const data = {
			"parents": [{"id": upFile.file.parentid}],
			"title": file.name
		};
    
		const mime = upFile.mime;	
		const uploadUrl = "https://www.googleapis.com/upload/drive/v2/files?uploadType=resumable";	
		const options = {
			url: uploadUrl,
			method: 'POST',
			timeout: 300000,
			headers: {
				"Authorization": `Bearer ${config.accessToken.access_token}`,
				"X-Upload-Content-Type": mime,
				"X-Upload-Content-Length": file.size
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
						const error = result.error.errors[0];
						const idx = error.message.indexOf("Media type")
						if( idx >= 0 ){
							callback("invalid mime");
							return;
						}
					}
				}else{
					logger.error("upload: unhandled error from getUploadResumableLinkCompleteCallback")
					logger.error(result);
				}
		
				// logger.debug "refreshing access token while getting resumable upload links"
		
				refreshAccessToken(
					() => {upFile.getUploadResumableLink(callback);}					
				);
			}else if(resp.statusCode == 200){
				upFile.uploadUrl = resp.headers.location;
				upFile.__uploadData__(0, callback);
			}else{
				callback(resp.statusCode);
			}
		}
	);
  }

	getNewRangeEnd(callback){
		const upFile = this;
		
		/* 
		ensure that the uploadUrl is not null. if it is, get a new resumable link
		*/
		if(!upFile.uploadUrl){
			upFile.getUploadResumableLink(callback);
			return;
		}
		
		const options = {
			url: upFile.uploadUrl,
			method: 'POST',
			headers: {
				"Authorization": `Bearer ${config.accessToken.access_token}`,
				"Content-Length": 0,
				"Content-Range": `bytes */${upFile.file.size}`
			}
		};
	
		request.post(options, function requestGetNewRangeEndCallback(err, resp, body){
			if( resp.statusCode == 308  ){
				const range = resp.headers.range || resp.headers.Range;
				if(!range){ 
					//sometimes, it doesn't return the range, so assume it is 0.
					// logger.error resp.headers
					// logger.error res
					setImmediate( upFile.__uploadData__.bind(upFile) ,0, callback);
					return;
				}
			
				const end = getRangeEnd(range);
				setImmediate( upFile.__uploadData__.bind(upFile), end, callback);
				return;
			}
	
			// unhandled case
			logger.debug("unhandled error with getting a new range end", resp.statusCode);
			setImmediate( callback, resp.statusCode, -1);
			return;
	
	
	
		});
	}

	upload(callback) {
		const upFile = this;
		const file = this.file;  
    
		// if the file is already being uploaded, don't try again.
		if ( upFile.uploading) {
			logger.debug(`${file.name} is already being uploaded`);
			callback("uploading");
			return;
		}
		
		if( !upFile.released){
		      throw(new Error("the file was not yet released. why are you uploading?!"));
		}

		upFile.uploading = true;
		magic.detectFile(upFile.uploadedFileLocation, function magicDetectFilesCallback(err, mime) {
			if (err) {
				logger.error("There was an error with detecting mime type", file.name);
				logger.error(err);
				callback(err);
				return;
			}
			
			// if the mime type is binary, set it to application/octect stream so google will accept it
			if (mime === 'binary') {
				mime = 'application/octet-stream';
			}
			
			upFile.mime = mime;
		
			logger.info( `Starting to upload file ${file.name}` );
			if (upFile.uploadUrl) {
				setImmediate( upFile.getNewRangeEnd.bind(upFile), callback);
			} else {
				setImmediate( upFile.getUploadResumableLink.bind(upFile), callback);
			}
			
		});
  }
  
  __uploadData__(start, callback){

    const upFile = this;
    const file = upFile.file;
    const mime = upFile.mime;
	
    const requestOptions = {
      method: "PUT",
      url: upFile.uploadUrl,
      headers:{
        "content-type": mime,
        "Authorization": `Bearer ${config.accessToken.access_token}`,
        "Content-Length": (file.size) - start,
        "Content-Range": `bytes ${start}-${file.size-1}/${file.size}`
      }
    };
    
	debugger;
    let once = false;
    
    // read the data
    const readStreamOptions = {
      start: start
    };  

    const rstream = fs.createReadStream( upFile.uploadedFileLocation, readStreamOptions);
    rstream.on('error', function(err){
      rstream.end();
    });
  
    const reqstream = request(requestOptions, function uploadRequestCallback(err, resp, body){  
  
      if(err) {
		logger.error(err);
        rstream.unpipe();
        rstream.pause();
        if(!once){
          once = true;
          setImmediate(upFile.getNewRangeEnd.bind(upFile), callback);         
        }     
        return;
      }
  
      if(resp.statusCode == 400 || resp.statusCode == 401 || resp.statusCode == 410){  
		logger.error(body)
        rstream.unpipe();
        rstream.pause();
        if(!once){
          once = true;
          setImmediate(upFile.getNewRangeEnd.bind(upFile), callback);         
        }     
        return;
      }
  
      if(resp.statusCode == 404){
        rstream.unpipe();
        rstream.pause();
        rstream.end();        
        if(!once){
          once = true;
          setImmediate(upFile.getNewRangeEnd.bind(upFile), callback);         
        }     
        return;
      }

      if(resp.statusCode == 308){ // success on resumable upload
        let rangeEnd = getRangeEnd(resp.headers.range);
        setImmediate(upFile.__uploadData__.bind(upFile), rangeEnd + 1, callback);
        return;
      }
  
      if ( 200 == resp.statusCode || resp.statusCode == 201){
        rstream.unpipe();
        rstream.pause();
		
		const result = JSON.parse(body);

		logger.info(`Finished uploading ${file.name}.`);
		file.downloadUrl = result.downloadUrl
		file.id = result.id
		file.size = parseInt(result.fileSize)
		file.ctime = (new Date(result.createdDate)).getTime()
		file.mtime =  (new Date(result.modifiedDate)).getTime()
		inodeTree.mapIdToObject(file.id, file);
		uploadTree.delete(file.inode);
    	saveUploadTree();
        fs.open(upFile.uploadedFileLocation, 'r', (err,fd)=>{
          setImmediate( upFile.postUploadProcessing.bind(upFile), fd, 0, callback);        
        });
        return
      }
  
  
      if (resp.statusCode >= 500){
        upFile.getNewRangeEnd( callback );
        return;
      }
  

      logger.error( "uncaugt state for file uploading" );
      logger.error( resp.statusCode );
      logger.error( resp.headers );
      logger.error( body     );
  
      upFile.getNewRangeEnd( callback);

    });
      
    rstream.pipe(
      reqstream
    );
  
  }


	
	toJson(){
		const self = this;
		return {
			filename: self.filename,
			parentid: self.parentid,
			released: self.released,
			inode: self.inode,
			uploadUrl: self.uploadUrl,
			newName: self.newName,
			newParent: self.newParent,
			toBeDeleted: self.toBeDeleted,
			uploadedFileLocation: self.uploadedFileLocation,
			mime: self.mime
		}
	}
	
	static fromJson(value){
		const self = new UploadingFile();
		self.filename = value.filename;
		self.parentid = value.parentid;
		self.released = value.released;
		self.newName = value.newName;
		self.toBeDeleted = value.toBeDeleted;
		self.uploadedFileLocation = value.uploadedFileLocation;
		self.mime = value.mime;		
		self.newParent = value.newParent;
		
		self.setInode(value.inode);
		self.setUploadUrl(value.uploadUrl, (err,stat)=>{
			uploadTree.delete(self.inode);
		});

		return self;
	}
	
	setInode(inode){
		this.inode = inode;
		this.file = inodeTree.getFromInode(inode);
		
		/*
		make sure that the name of the inode is the same as the uploading file or it's new name.
		*/
		if( this.file.name === this.newName || this.file.name === this.filename ){
			const parent = inodeTree.getFromId(this.parentid);
			const file = inodeTree.findChildByNameFromParentId(this.parentid, this.newName);
			if( file == null ){
				logger.error("There was an error with finding the proper file for the file being uploaded");
				logger.error(`${this.newName} not found in folder ${parent.name} with id ${parent.id}`)
			}else{
				this.file = file;
			}
		}
	}
	
};

//load upload Tree
if( fs.existsSync(pth.join(config.cacheLocation, 'data','uploadTree.json')) ){
	commonStatus.once('ready', ()=>{
		logger.info( "loading upload tree" );
		fs.readJson( pth.join(config.cacheLocation, 'data','uploadTree.json'), function readUploadTreeCallback(err, data){
			try{
				for( let key of Object.keys(data) ){
					let value = data[key];
					value.uploading = false;
					uploadTree.set( parseInt(key), UploadingFile.fromJson(value));
				}
			}catch (error){
				logger.error("There was an error parsing upload tree");
				logger.error(error);
			}
			UploadingFile.resumeUploadingFilesFromUploadFolder();
			saveUploadTree();
		});		
	});

}

/*
handle errors while uploading
*/
uploadQueue.on('error', (err, job)=>{
	console.log(err);
	console.log(job);
});

module.exports.UploadingFile = UploadingFile;
module.exports.saveUploadTree = saveUploadTree;
module.exports.uploadTree = uploadTree;
module.exports.uploadQueue = uploadQueue;