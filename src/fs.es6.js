"use strict";
const google = require( 'googleapis' );
const fs = require( 'fs-extra' );
const pth = require( 'path' );
const fuse = require( 'fusejs' );
const os = require( 'os' );
const PosixError = fuse.PosixError;

const inodeTree = require('./inodetree.js');

//require the client so that it will load the folder tree correctly.
const client = require('./client.es6.js');

const folder = require("./folder.es6.js");
const uploadTree = folder.uploadTree;
const GFolder = folder.GFolder;
const saveUploadTree = folder.saveUploadTree;
const f = require("./file.es6.js");
const GFile = f.GFile;
const addNewFile = f.addNewFile;
const queue_fn = f.queue_fn;
const queue = require('queue');

const exec = require('child_process').exec;

const common = require('./common.es6.js');
const config = common.config
const dataLocation = common.dataLocation;
const uploadLocation = common.uploadLocation;
const downloadLocation = common.downloadLocation;
const logger = common.logger;
const drive = common.GDrive

const q = queue({concurrency: config.maxConcurrentUploads || 4, timeout: 7200000 }) // default to 4 concurrent uploads


// http://lxr.free-electrons.com/source/include/uapi/asm-generic/errno-base.h#L23
const errnoMap = {
    EPERM: 1,
    ENOENT: 2,
    EIO: 5,
    ENXIO: 9,
    EACCESS: 13,
    EEXIST: 17,
    ENOTDIR: 20,
    EISDIR: 21,
    EINVAL: 22,
    ESPIPE: 29,
    ENOTEMPTY: 39
};

/*
 ################################################
 ####### Filesystem Handler Functions ###########
 ################################################
 */
class GDriveFS extends fuse.FileSystem{

    getattr(context, inode, reply){
        if (inodeTree.has(inode)){
            inodeTree.getFromInode(inode).getAttr(
                function getAttrCallback(status, attr){
                    reply.attr(attr, 5)
                }
            );

        }else{
            reply.err(errnoMap.ENOENT)
        }
    }

    opendir(context, inode, fileInfo, reply){
        reply.open(fileInfo);
    }

    releasedir(context, inode, fileInfo, reply){
        // console.log('Releasedir was called!');
        // console.log(fileInfo);
        reply.err(0);
    }

    /*
     * Handler for the readdir() system call.
     * path: the path to the file
     * cb: a callback of the form cb(err, names), where err is the Posix return code
     *     and names is the result in the form of an array of file names (when err === 0).
     */
    readdir(context, inode, requestedSize, offset, fileInfo, reply){
        if(inodeTree.has(inode)){
            const object = inodeTree.getFromInode(inode);
            if(object instanceof GFile){
                reply.err( errnoMap.ENOTDIR)
            }else if (object instanceof GFolder){
                const size = Math.max( requestedSize , object.children.length * 256);
                // size = requestedSize
                const parent = inodeTree.getFromId(object.parentid);
                var totalSize = 0;
                // totalSize += reply.addDirEntry('.', requestedSize, {inode: object.inode}, offset);
                // totalSize += reply.addDirEntry('..', requestedSize, {inode: parent.inode}, offset);
                for( let child of object.children ){
                    const cnode = inodeTree.getFromInode(child);
                    if(cnode){
                        const attr = cnode.getAttrSync();
                        //console.log( cnode.name, cnode.inode);
                        const len = reply.addDirEntry(cnode.name, size, cnode, offset);
                        totalSize += len
                    }
                }

                if( object.children.length == 0){
                    reply.buffer(new Buffer(0), 0);
                }else{
                    reply.buffer(new Buffer(0), requestedSize);
                }
            }else{
                reply.err(errnoMap.ENOENT)
            }
        }else{
            reply.err(errnoMap.ENOENT)
        }
    }

    setattr(context, inode, attrs, reply){
        logger.debug( `setting attr for ${inode}`);
        logger.silly(attrs);
        const file = inodeTree.getFromInode(inode);
        if(!file){
            reply.err(errnoMap.ENOENT);
            return;
        }
        // console.log file
        // console.log attrs
        const a = new Date(attrs.atime);
        const m = new Date(attrs.mtime);
        // console.log a.getTime(),m.getTime()
        // attrs.atime = a.getTime()
        // attrs.mtime = m.getTime()
        file.mtime = m.getTime()
        if (attrs.hasOwnProperty("size")){
            file.size = attrs.size
        }

        if (attrs.hasOwnProperty("mode")){
            logger.debug(`mode before and after: ${file.mode}-${attrs.mode}`)
            file.mode = attrs.mode
        }

        inodeTree.map.set( inode, file );


        reply.attr(file.getAttrSync(), 5);
        // reply.err(0)
    }

    open(context, inode, fileInfo, reply){
        const self = this;
        const flags = fileInfo.flags;
        if (flags.rdonly){ //read only
            if (inodeTree.has(inode)){
                const file = inodeTree.getFromInode(inode);
                if (file instanceof GFile){
                    if (file.downloadUrl){ //make sure that the file has been fully uploaded
                        reply.open(fileInfo);
                    }else{
                        //wait for filesystem to finish uploading file and retry again
                        reply.err(PosixError.EACCES);
                    }
                    return;
                }else{
                    reply(errerrnoMap.EISDIR);
                    return;
                }
            }else{
                reply.err( errnoMap.ENOENT );
            }
            return;
        }

        if( flags.wronly ){ //write only
            logger.silly(`$tried to open file "${inode}" for writing`);
            if ( inodeTree.has(inode) ){ //if folderTree has path, make sure it's a file with size zero
                const file = inodeTree.getFromInode(inode);
                if (file instanceof GFile){
                    if (file.size == 0){
                        // logger.debug(`${path} size was 0`);
                        if (uploadTree.has(inode)){
                            const cache = uploadTree.get(inode).cache;
                            fs.open( pth.join(uploadLocation, cache), 'w+', function openFileForWritingCallback(err,fd){
                                if (err){
                                    logger.debug( "could not open file for writing" );
                                    logger.debug( err );
                                    reply.err( -err.errno );
                                    return;
                                }

                                fileInfo.file_handle = fd;
                                reply.open(fileInfo);
                            });
                        }else{
                            reply.err( errnoMap.EACCESS);
                        }

                    }else{
                        reply.err(errnoMap.EACCESS);
                    }
                }else{
                    reply.err(errnoMap.EISDIR);
                }
            }else{
                reply.err( errnoMap.ENOENT);
            }
            return
        }
        /*
         if (flags.rdwr){ // if it doesn't have the path, create the file
         reply.err(errnoMap.ENOENT);
         return;

         var parent = folderTree.get( pth.dirname(path) );
         if ( parent && parent instanceof GFolder){
         var now = ( new Date()).getTime();
         var name = pth.basename(path)

         var file = new GFile(null, null, parent.id, name, 0, now, now, true)
         folderTree.set( path, file );
         var upFile = {
         cache: cache,
         uploading: false
         };
         uploadTree.set(  path, upFile );
         saveUploadTree();

         if (parent.children.indexOf(name) < 0);
         (parent.children.push name);

         fs.open pth.join(uploadLocation, cache), 'w', (err,fd) ->
         if err
         cb -errnoMap[err.code]
         else
         cb 0, fd
         return

         return
         else
         cb -errnoMap.EPERM
         return

         cb(-errnoMap.ENOENT)
         return
         */

        if (flags.rdwr){ //read/write
            logger.debug( `tried to open file "${path}" for r+w`);
            reply.err( errnoMap.ENOENT );
        }

        return;
    }

    read(context, inode, len, offset, fileInfo, reply){
        // logger.silly( `reading file ${path} - ${offset}:${len}`);
        var once = false
        function readDataCallback(dataBuf){
            if(!once){
                once = true;
                if(Buffer.isBuffer(dataBuf)){
                    reply.buffer(dataBuf, dataBuf.length);
                }else{
                    reply.err(errnoMap.EIO);
                }
            }
        }

        if( inodeTree.has(inode)){
            // make sure that we are only reading a file
            const file = inodeTree.getFromInode(inode)
            if (file instanceof GFile ){

                // make sure the offset request is not bigger than the file itself
                if (offset < file.size){
                    file.read(offset, offset+len-1,true,readDataCallback);
                }else if(offset == file.size){
                    reply.err(0);
                }else{                    
                    reply.err(errnoMap.ESPIPE);
                }
            }else{
                reply.err(errnoMap.EISDIR)
            }

        }else{
            reply.err(errnoMap.ENOENT)
        }
    }

    write(context, inode, buffer, position, fileInfo, reply){

        // path = inodeToPath.get inode
        // logger.silly( `writing to file ${path} - position: ${position}, length: ${buffer.length}"

        const file = inodeTree.getFromInode( inode )
        if (!file){
            logger.debug( inode );
            reply.err( errnoMap.ENOENT );
            return;
        }
        const size = file.size
        fs.write( fileInfo.file_handle, buffer, 0, buffer.length, position, function fsWriteCallback(err, bytesWritten, buffer){
            if (err){
                logger.debug( `there was an error writing for file ${file.name}` )
                logger.debug( err )
                logger.debug( "position", position, "fh", fileInfo.file_handle )
                reply.err(err.errno);
                return;
            }

            // it is simportant to update the file size as we copy in to it. sometimes, cp and mv will check the progress by scanning the filesystem
            if ( size < (position + buffer.length) ){
                file.size = position + buffer.length
            }
            reply.write(bytesWritten);
        });
    }

    flush(context, inode, fileInfo, reply){
        reply.err(0);
    }

    /*
     * Handler for the mkdir() system call.
     * path: the path of the new directory
     * mode: the desired permissions of the new directory
     * cb: a callback of the form cb(err), where err is the Posix return code.
     */
    mkdir(context, parentInode, name, mode, reply){
        // parentPath = inodeToPath.get parentInode
        // path = pth.join parentPath, name
        // logger.debug(`creating folder ${path}");
        logger.debug( `creating folder ${name}` );
        const parent = inodeTree.getFromInode( parentInode);
        if( parent ){ //make sure that the parent exists
            if (parent instanceof GFolder){ //make sure that the parent is a folder

                for( let childInode of parent.children){ // make sure that the child doesn't already exist
                    const child = inodeTree.getFromInode(childInode)
                    if (child && child.name === name ){
                        reply.err(errnoMap.EEXIST);
                        return;
                    }
                }

                const folder = {
                    resource:{
                        title: name,
                        mimeType: 'application/vnd.google-apps.folder',
                        parents: [{id: parent.id}]
                    }
                };

                drive.files.insert(folder, function createFolderCallback(err, res){
                    if (err){
                        logger.log( "error", err );
                        reply.err(errnoMap.EIO);
                        return;
                    }else{
                        const now = (new Date).getTime()

                        const folder = new GFolder(res.id, res.parents[0].id, name, (new Date(res.createdDate)).getTime(), (new Date(res.modifiedDate)).getTime(), res.editable, [])
                        inodeTree.insert(folder);
                        const attr = folder.getAttrSync();
                        let entry = {
                            inode: attr.inode,
                            generation: 2,
                            attr: attr,
                            attr_timeout: 5,
                            entry_timeout: 5
                        };
                        reply.entry(entry);
                    }
                });
            }else{
                reply.err(errnoMap.ENOTDIR)
            }
        }else{
            reply.err(errnoMap.ENOENT)
        }
    }

    /*
     * Handler for the rmdir() system call.
     * path: the path of the directory to remove
     * cb: a callback of the form cb(err), where err is the Posix return code.
     */
    rmdir(context, parentInode, name, reply) {
        const parent = inodeTree.getFromInode(parentInode);
        logger.debug( `removing folder ${name}` );

        // make sure the actual directory exists
        for (let childInode of parent.children) {
            const folder = inodeTree.getFromInode(childInode);
            if (folder.name === name) {

                //make sure that it is a folder
                if (folder instanceof GFolder) {
                    //make sure it is empty
                    if (folder.children.length == 0) {
                        drive.files.trash({fileId: folder.id}, function removeDirCallback(err, res) {
                            if (err) {
                                logger.error( `unable to remove folder ${path}`);
                                reply.err(errnoMap.EIO);
                                return;
                            }
                            const idx = parent.children.indexOf(childInode);
                            if (idx >= 0) {
                                parent.children.splice(idx, 1);
                            }
                            inodeTree.delete(childInode)

                            reply.err(0)
                            inodeTree.saveFolderTree();
                        });
                        return;
                    } else {
                        reply.err(errnoMap.ENOTEMPTY);
                        return;
                    }
                } else {
                    reply.err(errnoMap.ENOTDIR);
                    return;
                }
            }
        }

        reply.err(errnoMap.ENOENT);
    }

    mknod(context, parentInode, name, mode, rdev, reply){

        const parent = inodeTree.getFromInode(parentInode);

        for(childInode in parent.children){ //TODO: if file exists, delete it first
            const child = inodeTree.getFromInode(childInode);
            if (child && child.name === name){
                reply.err(PosixError.EEXIST);
                return;
            }
        }

        const now = (new Date).getTime();

        const file = new GFile(null, null, parent.id, name, 0, now, now, inode, true)
        let inode = inodeTree.insert( file );

        logger.debug (`mknod: parentid: ${parent.id} -- inode ${inode}` );
        logger.info  (`adding a new file ${name} to folder ${parent.name}` );
        const attr = file.getAttrSync();

        const upFile ={
            cache: file.getCacheName(),
            uploading: false
        }
        uploadTree.set( inode, upFile);
        saveUploadTree();


        entry = {
            inode: attr.inode,
            generation: 2,
            attr: attr
            //attr_timeout: 30,
            //entry_timeout: 60
        };

        reply.entry(entry);
        return;
    }
    


    create(context, parentInode, name, mode, fileInfo, reply){
        const parent = inodeTree.getFromInode(parentInode);

        if (parent){ //make sure parent exists
            logger.debug( `creating file ${name}`);

            // check to see if a file exists with the same name in the folder tree
            for (let childInode of parent.children){ 
                // TODO: if file exists, delete it first
                const obj = inodeTree.getFromInode(childInode);
                if(obj instanceof GFile){
                    if(obj.name === name){
                        reply.err(PosixError.EEXIST);
                        return;
                    }
                }
            }
            const now = (new Date).getTime();
            const file = new GFile(null, null, parent.id, name, 0, now, now, true);
            const cache = file.getCacheName();
            const systemPath = pth.join(uploadLocation, cache);

            logger.debug( `adding file "${name}" to folder "${parent.name}"`);

            const inode = inodeTree.insert(file);
            parent.children.push(inode);


            logger.debug( `create: parentid: ${parent.id} -- inode ${inode}`);
            logger.info (`adding a new file ${name} to folder ${parent.name}`);

            inodeTree.saveFolderTree();

            fs.open( systemPath, 'w', function createOpenFileCallback(err, fd){
                if (err){
                    logger.error( `unable to create file ${inode} -- ${name}, ${err}` );
                    reply.err(errnoMap[err.code]);
                    return;
                }
                fileInfo.file_handle = fd;
                logger.debug( "setting upload Tree" );
                const upFile = {
                    cache: cache,
                    uploading: false
                };
                uploadTree.set( inode, upFile );
                saveUploadTree()
                const attr = {
                    inode: inode, //#parent.inode,
                    generation: 1,
                    attr:file
                };
                reply.create( attr, fileInfo );
                return;
            });
        }else{
            reply.err( errnoMap.ENOENT );
        };
    }
    /*
     * Handler for the unlink() system call.
     * path: the path to the file
     * cb: a callback of the form cb(err), where err is the Posix return code.
     */
    unlink(context, parentInode, name, reply){
        const parent = inodeTree.getFromInode( parentInode );

        for( let childInode of parent.children ){
            const file = inodeTree.getFromInode(childInode)

            // make sure the file still exists in the inodeTree
            // if not, remove it
            if(!file){
                const idx = parent.children.indexOf(childInode);
                parent.children.splice(idx,1);
                continue;
            }

            // make sure it's the right file
            if(file.name != name){
                continue;
            }

            if(file instanceof GFolder){
                reply.err (errnoMap.EISDIR);
                return;
            }

            //now we are pretty sure that the inode is the correct one
            logger.debug( `removing file ${name}`);
            parent.children.splice( parent.children.indexOf(childInode), 1)
            inodeTree.delete( childInode );
            inodeTree.saveFolderTree();

            drive.files.trash( {fileId: file.id}, function deleteFileCallback(err, res){
                if (err){
                    logger.debug( `unable to remove file ${file.name}` );
                }
                reply.err(0) //TODO: handle case when google fails to delete a file
            });

            if (uploadTree.has( childInode )){
                const cache = uploadTree.get(childInode).cache;
                uploadTree.delete(childInode)
                fs.unlink(pth.join(uploadLocation,cache), function unlinkDeleteFileCallback(err){});
            }

            return;
        }

        reply.err(PosixError.ENOENT);
    }


    /*
     * Handler for the release() system call.
     * path: the path to the file
     * fd:  the optional file handle originally returned by open(), or 0 if it wasn't
     * cb: a callback of the form cb(err), where err is the Posix return code.
     */
    release(context, inode, fileInfo, reply){
        logger.silly(`closing file ${inode}`);
        if (uploadTree.has (inode) ){
            logger.debug(`${inode} was in the upload tree`);
            // close the file
            fs.close( fileInfo.file_handle, function closeFileCallback(err){
                if (err){
                    reply.err(err.errno);
                    return;
                }
                reply.err(0);

                // upload file once file is closed
                if(uploadTree.has(inode)){
                    const upCache = uploadTree.get(inode);
                    upCache.released = true;
                    uploadTree.set(inode, upCache);
                    saveUploadTree();

                    const file =   inodeTree.getFromInode(inode);
                    const parent = inodeTree.getFromId(file.parentid);
                    /*
                     three cases:
                     if file size is 0: delete it and don't upload
                     if file size is <=10MB, just upload it directly
                     if file size is >10 MB, add to upload queue
                     */


                    if( 0 < file.size &&  file.size <= 10485760){ //10MB
                        parent.upload( file.name, inode, uploadCallback(inode, function(){})    );
                    }else if(file.size >  10485760 ){}
                    q.push(
                        function uploadQueueFunction(cb){
                            if( parent instanceof GFile){
                                logger.debug(`While uploading, ${name} was a file - ${parent}`);
                                cb();
                                return;
                            }
                            parent.upload(file.name, inode, uploadCallback(inode,cb))
                            return
                        }
                    );

                    q.start()
                }else{
                    uploadTree.delete(inode);
                    saveUploadTree();
                }
            });
        }else if (fileInfo.file_handle){
            fs.close(fileInfo.file_handle, function closeFileCallback(err){
                if (err){
                    logger.error("There was an error closing file");
                    logger.error(err);
                    reply.err(err.errno);
                    return;
                }
                reply.err(0);
            });
        }else{
            reply.err(0);
        }
    }

    statfs(context, inode, reply){
        reply.statfs( {
            bsize: Math.floor(config.chunkSize/2),
            iosize: Math.floor(config.chunkSize/2),
            frsize: Math.floor(config.chunkSize/2),
            blocks: 1000000,
            bfree: 1000000,
            bavail: 1000000,
            files: 1000000,
            ffree: 1000000,
            favail: 1000000,
            fsid: 1000000,
            flag: 0
        });
    }

    getxattr(context, parentInode, name, size, position, reply){
        console.log('GetXAttr was called!');
        const parent = inodeToPath.get(parentInode)
        for( childInode of parent.children){
            if(inodeTree.getFromInode(childInode).name === name){
                reply.err(0);
                return;
            }
        }
        reply.err( PosixError.ENOENT);
    }

    listxattr(context, inode, size, reply){
        console.log("listxattr called");
        const obj = inodeTree.getFromInode(inode);
        if (obj){
            // console.log(obj);
        }

        reply.xattr( 1024*1024 );
    }

    access(context, inode, mask, reply){
        // console.log('Access was called!');
        if(inodeTree.has(inode)){
            reply.err(0);
            return;
        }
        reply.err(errnoMap.EACCESS);
        return;
    }

    rename(context, oldParentInode, oldName, newParentInode, newName, reply){
        //find the currrent child
        const parent = inodeTree.getFromInode(oldParentInode);
        if(!parent){
            reply.err(PosixError.ENOENT);
            return;
        }

        for( let childInode of parent.children){
            const child = inodeTree.getFromInode(childInode);
            if(!child){
				parent.children.splice(parent.children.indexOf(childInode), 1);
                continue;
            }
            if (child.name === oldName){
                // move to new folder if required
                const params = {
                    resource:{
                        title: newName
                    },
                    fileId: child.id,
                    modifiedDate: true
                };
                if( newParentInode != oldParentInode ){
                    const newParent = inodeTree.getFromInode(newParentInode);
                    const oldParent = parent;
                    if( !newParent ){
                        reply.err (PosixError.ENOENT);
                        return;
                    }
                    if(  !(newParent instanceof GFolder)){
                        reply.err (PosixError.ENOTDIR);
                        return;
                    }
                    params.addParents = newParentInode.id;
                    params.removeParents =  parent.id;
                }

                child.name = newName;
                console.log ("before google api");
                drive.files.patch( params, function filesPatchCallback(err){
                    console.log("after google api");
                    if (err){
                        logger.error( `There was an error with renaming file ${child.name}` );
                        logger.error( err );
                        reply.err (PosixError.EIO);
                        return
                    }
                    reply.err(0);
                    if (newParentInode != oldParentInode){
                        newParent.children.push (childInode);
                        oldParent.children.splice( oldParent.children.indexOf(childInode), 1 );
                    }
                });
                return;
            }
        }

        // if we get here, it means there was no child found
        reply.err( PosixError.ENOENT)
    }

    lookup(context, parentInode, name, reply){

        //make sure the parent inode exists
        if( !inodeTree.has(parentInode)){
            reply.err(PosixError.ENOENT);
            return;
        }

        const parent = inodeTree.getFromInode( parentInode );
        for( let childInode of parent.children){
            const child = inodeTree.getFromInode(childInode);
            if (child && child.name === name){
                const attr = child.getAttrSync();
                attr.size = attr.size || 4096
                const entry = {
                    inode: childInode,
                    generation: 2,
                    attr: attr,
                    // attr_timeout: 5,
                    // entry_timeout: 5
                };
                reply.entry(entry);
                return;
            }
        }

        //if the child is not found
        reply.err(PosixError.ENOENT);

    }
}


function moveToDownload (file, fd, uploadedFileLocation, start,cb){

    const end = Math.min(start + config.chunkSize, file.size)-1
    const savePath = pth.join(config.cacheLocation, 'download', `${file.id}-${start}-${end}`);
    const rstream = fs.createReadStream(uploadedFileLocation, {fd: fd, autoClose: false, start: start, end: end})
    const wstream = fs.createWriteStream(savePath)

    rstream.on('end',  function moveToDownloadReadStream(){

        start += config.chunkSize;
        wstream.end();
        if (start < file.size){
            moveToDownload(file, fd, uploadedFileLocation, start, cb);
            return;
        }
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
                logger.debug(`unable to close file after transffering ${uploadedFile}`);
                cb();
                return;
            }
            fs.unlink( uploadedFileLocation, function deleteUploadedFile(err){
                if (err){
                    logger.error( `unable to remove file ${uploadedFile}`)
                }
                cb();
            });
        });
    });

    rstream.pipe(wstream);
}

//function to create a callback for file uploading
function uploadCallback(inode, cb){
    return function (err, result){
        if(err){
            if (err === "invalid mime"){
                logger.debug(`the mimetype of ${inode} was invalid`);
                cb();
                return
            }
            if(err === "uploading"){
                cb();
                return;
            }
            if(err.code === "ENOENT"){
                uploadTree.delete(inode);
                cb();
                return;
            }


            cb();
            const file = inodeTree.getFromInode(inode);
            logger.debug(`Retrying upload: "${file.name}".`);
            q.push(
                function uploadFunctionQueued(_cb){
                    parent.upload(file.name, inode , uploadCallback(inode,_cb));
                    return;
                }
            );
            q.start()
        }

        const file = inodeTree.getFromInode(inode)
        if(!file){
            // TODO: Sometimes, a file is not found. Although, it shouldn't have been deleted.
            debugger;
            logger.error(`inode ${inode} was not found in inodetree anymore`);
            uploadTree.delete(inode);
            cb();
            return; 
        }
        const parent = inodeTree.getFromId(file.parentid)
        const upFile = uploadTree.get(inode)

        if(!upFile){ //#make sure uploaded file is still in the uploadTree
            cb();
            return;
        }
        const uploadedFileLocation = pth.join( uploadLocation, upFile.cache);

        logger.info( `successfully uploaded ${file.name}`);

        uploadTree.delete(inode);
        saveUploadTree();
        if (inodeTree.has(inode)){
            const file = inodeTree.getFromInode(inode);
            inodeTree.mapInodeToId(inode, result.id);
            logger.debug(`${file.name} already existed in inodeTree`);
            file.downloadUrl = result.downloadUrl
            file.id = result.id
            file.size = parseInt(result.fileSize)
            file.ctime = (new Date(result.createdDate)).getTime()
            file.mtime =  (new Date(result.modifiedDate)).getTime()
        }else{
            logger.debug(`${file.name} folderTree did not exist`);
            common.currentLargestInode++;
            let inode = common.currentLargestInode;
            const file = new GFile(result.downloadUrl, result.id, result.parents[0].id, result.title, parseInt(result.fileSize), (new Date(result.createdDate)).getTime(), (new Date(result.modifiedDate)).getTime(), inode, true)
            inodeTree.insert(file)
        }
        // update parent
        if( !(file.inode in parent.children)){
            parent.children.push(file.inode)
        }
        inodeTree.saveFolderTree();

        // move the file to download folder after finished uploading
        fs.open( uploadedFileLocation, 'r', function openFileAfterUploadCallback(err,fd){
            if(err){
                logger.debug( `could not open ${uploadedFileLocation} for copying file from upload to uploader` );
                logger.debug( err );
                return;
            }

            moveToDownload(file, fd, uploadedFileLocation, 0, cb)
        });

    };
}

function recurseResumeUploadingFilesFromUploadFolder(inode, files){
    const file = inodeTree.getFromInode(inode)

    /* make sure that the inode is a file */
    if( file instanceof GFile ){        
        const cache = file.getCacheName();

        /* ensure that this file is in the list of files to be checked */
        if(files.has(cache)){
            const upFile = {
                cache: cache,
                uploading: false,
                released: true
            };
            files.delete(cache);

            /* make sure that this inode is not already in the upload tree */
            if( !uploadTree.has(inode) ){

                /* make sure that the file is the same size as what's been reported in the inodeTree */
                fs.stat(pth.join(uploadLocation,cache), function(err, stat){
                    if(err){
                        logger.debug("There was an error while stating lost upload");
                        logger.debug(err);
                        return;
                    }
                    if(stat.size == file.size ){
                        const parent = inodeTree.getFromId(file.parentid);

                        // ensure that the parent is a folder
                        if(parent instanceof GFolder){

                            logger.debug("a lost uploading file was found: ", file.name );

                            // everything is good now, so we can enqueue the uploading
                            uploadTree.set( inode, upFile );
                            q.push(
                                function uploadQueueFunction(cb){
                                    if( parent instanceof GFile){
                                        logger.debug(`While uploading, ${name} was a file - ${parent}`);
                                        cb();
                                        return;
                                    }
                                    parent.upload(file.name, inode, uploadCallback(inode,cb))
                                    return
                                }
                            );

                            q.start();
                        }
                    }
                });
            }

        }
    }

    if( files.size > 0 && inode > 0){
        setImmediate( function (){
            recurseResumeUploadingFilesFromUploadFolder(inode - 1, files);
        });
    }else{
        saveUploadTree();
    }
    
}

function resumeUploadingFilesFromUploadFolder(){
    /*
    
    sometimes, the uploadTree.json file will get corrupted.
    as a safeguard, read the files list from the upload data folder and try to find 
    the associated inode and restart the upload.

    */

    fs.readdir(uploadLocation, function (err, files){
        const filesToBeUploaded = new Set(files);

        recurseResumeUploadingFilesFromUploadFolder(inodeTree.currentLargestInode, filesToBeUploaded);
    });
}

// resume file uploading
function resumeUpload(){
    // uploadWork = null
    if (uploadTree.size > 0){
        logger.info( "resuming file uploading" );
        for(let inode of uploadTree.keys()){

            if( !inodeTree.has(inode) ){
                uploadTree.delete(inode);
                continue;
            }
            let file = inodeTree.getFromInode(inode);

            // check to see if the file was released by the filesystem
            // if it wasn't released by the filesystem, it means that the file was not finished transfering
            let value = uploadTree.get(inode);
            if(value.released){
                let parent = inodeTree.getFromId( file.parentid );
                value.uploading = false;
                if (parent){
                    if (parent instanceof GFolder){
                        //value.location = false;
                        uploadTree.set(inode, value);

                        q.push(
                            function resumeUploadQueueFunction(cb){
                                parent.upload(file.name, inode, uploadCallback(inode,cb));
                            }
                        )
                        q.start();
                        continue;
                    }else{
                        logger.debug(`While resuming uploads, ${parent} was not a folder`);
                    }
                }
            }else{
                inodeTree.delete(inode)
                uploadTree.delete(inode)
                let parentInode = inodeTree.getFromId(value.parentid);
                let parent = inodeTree.getFromInode(parentInode)
                if (parent){
                    let idx = parent.children.indexOf(inode);
                    if (idx > 0){
                        parent.children.splice(idx, 1);
                    }
                }
                let path = pth.join(uploadLocation, value.cache);
                fs.unlink(path, function(){});
            }
        };
        saveUploadTree();

    }

    setTimeout(resumeUploadingFilesFromUploadFolder, 5000);

}

function start(count){
    if( inodeTree.map.size > 1){
        try{
            logger.info('attempting to start f4js');
            var add_opts;
            var command;
            switch (os.type()){
                case 'Linux':
                    add_opts = ["-o", "allow_other", ]
                    command = `umount -f ${config.mountPoint}`
                    break;
                case 'Darwin':
                    add_opts = ["-o",'daemon_timeout=0', "-o", "noappledouble", "-o", "noubc"];
                    command = `diskutil umount force ${config.mountPoint}`
                    break
                default:
                    add_opts = []
                    command = `fusermount -u ${config.mountPoint}`
            }
            if( process.version < '0.11.0'){
                opts.push( "-o", "allow_other")
            }

            const debug = false;

            exec( command, function unmountCallback(err, data){
                try{
                    fs.ensureDirSync(config.mountPoint);
                }catch(e){
                    logger.error("could not ensure the mountpoint existed.");
                    process.exit(1)
                }
                if (err){
                    logger.error( "unmount error:", err);
                }
                if (data){
                    logger.info( "unmounting output:", data);
                }
                const opts =  ["GDrive", "-o",  "allow_other", config.mountPoint];
                // opts.push "-s"
                // opts.push "-f"

                // opts.push "-mt"
                // opts.push("-d")
                fuse.fuse.mount({
                    filesystem: GDriveFS,
                    options: opts.concat(add_opts)
                });

                logger.log('info', `mount point: ${config.mountPoint}`);
                setTimeout(resumeUpload, 120000);
                return;
            });
        }catch(e){
            logger.log( "error", `Exception when starting file system: ${e}`);
        }
    }else{
        setTimeout(start, 500);
    }
}


start(0)
