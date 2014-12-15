google = require 'googleapis'
fs = require 'fs-extra'
winston = require 'winston'
rest = require 'restler'
hashmap = require( 'hashmap' ).HashMap
pth = require 'path'
fuse = require 'fusejs'
os = require 'os'
MD5 = require 'MD5'
PosixError = fuse.PosixError;


client = require('./client')
folderTree = client.folderTree
drive = client.drive
folder = require("./folder")
uploadTree = folder.uploadTree
GFolder = folder.GFolder
saveUploadTree = folder.saveUploadTree
inodeToPath = client.inodeToPath
f = require("./file");
logger = f.logger
GFile = f.GFile
queue = require 'queue'

{exec} = require('child_process')

#read input config
if fs.existsSync 'config.json'
  config = fs.readJSONSync 'config.json'
else
  config = {}


config.mountPoint ||= "/tmp/mnt"
config.cacheLocation ||=  '/tmp/cache'
GFile.chunkSize ||= config.chunkSize 8388608 #8MB default
GFile.GDrive = client.drive;
GFolder.uploadChunkSize ||= 16777216 #16MB default
uploadLocation = pth.join config.cacheLocation, 'upload'
q = queue({concurrency: config.maxConcurrentUploads || 4, timeout: 7200000 }) #default to 4 concurrent uploads


#http://lxr.free-electrons.com/source/include/uapi/asm-generic/errno-base.h#L23
errnoMap =
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



###############################################
####### Filesystem Helper Functions ###########
###############################################

# /*
#  * Converts numerical open() flags to node.js fs.open() 'flags' string.
#  */
# convertOpenFlags = (openFlags) ->
#   switch (openFlags & 3) {
#   case 0:
#     return 'r';              // O_RDONLY
#   case 1:
#     return 'w';              // O_WRONLY
#   case 2:
#     return 'r+';             // O_RDWR
#   }


################################################
####### Filesystem Handler Functions ###########
################################################

class GDriveFS extends fuse.FileSystem

  getattr: (context, inode, reply) ->
    path = inodeToPath.get inode
    console.log "getattr inode#: #{inode} which is #{path}"
    if folderTree.has(path)
      callback = (status, attr)->
        reply.attr(attr, 30)
        return
      folderTree.get(path).getAttr(callback)
        
    else
      reply.err(-errnoMap.ENOENT)
    return

  opendir: (context, inode, fileInfo, reply) ->
      console.log('Opendir was called!');
      # //reply.err(0);
      reply.open(fileInfo);

  releasedir: (context, inode, fileInfo, reply) ->
      console.log('Releasedir was called!');
      console.log(fileInfo);
      reply.err(0);

  # /*
  #  * Handler for the readdir() system call.
  #  * path: the path to the file
  #  * cb: a callback of the form cb(err, names), where err is the Posix return code
  #  *     and names is the result in the form of an array of file names (when err === 0).
  #  */
  readdir: (context, inode, requestedSize, offset, fileInfo, reply) ->
    path = inodeToPath.get inode
    console.log "readdir #{path}, #{offset}, #{requestedSize}"
    # console.log path
    if folderTree.has(path)
      object = folderTree.get(path)
      if object instanceof GFile
        reply.err -errnoMap.ENOTDIR
      else if object instanceof GFolder
        size = Math.max( requestedSize , object.children.length * 64)
        # size = requestedSize
        parent = folderTree.get pth.dirname(path)
        totalSize = 0
        totalSize += reply.addDirEntry('.', requestedSize, {inode: object.inode}, offset);
        totalSize += reply.addDirEntry('..', requestedSize, {inode: parent.inode}, offset);
        for child in object.children
          cpath = pth.join(path,child)
          cnode = folderTree.get cpath
          attr = cnode.getAttrSync()
          len = reply.addDirEntry(child, requestedSize, {inode: cnode.inode}, offset);          
          totalSize += len
        reply.buffer(new Buffer(0), requestedSize)
      else
        reply.err -errnoMap.ENOENT
    else
      reply.err -errnoMap.ENOENT
    return

  open: (context, inode, fileInfo, reply) ->
    console.log('Open was called!');
    # console.log  fileInfo
    path = inodeToPath.get inode
    parent = folderTree.get pth.dirname(path)
    flags = fileInfo.flags
    if flags.rdonly #read only
      if folderTree.has(path)
        file = folderTree.get(path)
        if file instanceof GFile
          if file.downloadUrl #make sure that the file has been fully uploaded
            reply.open(fileInfo)
          else
            reply.err -errnoMap.EACCESS
        else
          reply.err-errnoMap.EISDIR
      else
        reply.err -errnoMap.ENOENT
      return

    if flags.wronly #write only
      cache = MD5(path)
      logger.log 'debug', "tried to open file \"#{path}\" for writing"
      reply.err -errnoMap.ENOENT
      if folderTree.has(path) #if folderTree has path, make sure it's a file with size zero
        file = folderTree.get(path)
        if (file instanceof GFile)
           if file.size == 0
            logger.debug "#{path} size was 0"
            fs.open pth.join(uploadLocation, cache), 'w', (err,fd) ->
              if err
                logger.debug "could not open file for writing"
                logger.debug err
                cb -errnoMap[err.code]
              else
                cb 0, fd
              return

           else
             cb -errnoMap.EACCESS
        else
          cb -errnoMap.EISDIR
        return
      else #if it doesn't have the path, create the file
        parent = folderTree.get pth.dirname(path)
        if parent and parent instanceof GFolder
          now = ( new Date()).getTime()
          name = pth.basename(path)

          file = new GFile(null, null, parent.id, name, 0, now, now, true)
          folderTree.set path, file
          upFile = 
            cache: cache
            uploading: false
          uploadTree.set path, upFile
          saveUploadTree()

          if parent.children.indexOf(name) < 0
            parent.children.push name

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

    if flags.rdwr #read/write
      logger.log 'info', "tried to open file \"#{path}\" for r+w"
      reply.err -errnoMap.ENOENT

    return
  # /*
  #  * Handler for the read() system call.
  #  * path: the path to the file
  #  * offset: the file offset to read from
  #  * len: the number of bytes to read
  #  * buf: the Buffer to write the data to
  #  * fh:  the optional file handle originally returned by open(), or 0 if it wasn't
  #  * cb: a callback of the form cb(err), where err is the Posix return code.
  #  *     A positive value represents the number of bytes actually read.
  #  */
  read: (context, inode, len, offset, fileInfo, reply) ->
    path = inodeToPath.get inode
    logger.log "silly", "reading file #{path} - #{offset}:#{len}"

    if folderTree.has(path)
      callback = (dataBuf) ->
        reply.buffer(dataBuf, dataBuf.length)
        return

      #make sure that we are only reading a file
      file = folderTree.get(path)
      if file instanceof GFile

        #make sure the offset request is not bigger than the file itself
        if offset < file.size
          file.read(offset, offset+len-1,true,callback)
        else
          reply.err(-errnoMap.ESPIPE)
      else
        reply.err(-errnoMap.EISDIR)

    else
      reply.err(-errnoMap.ENOENT)



    return

  # /*
  #  * Handler for the write() system call.
  #  * path: the path to the file
  #  * position: the file offset to write to
  #  * len: the number of bytes to write
  #  * buf: the Buffer to read data from
  #  * fd:  the optional file handle originally returned by open(), or 0 if it wasn't
  #  * cb: a callback of the form cb(err), where err is the Posix return code.
  #  *     A positive value represents the number of bytes actually written.
  #  */
  write: (path, position, len, buf, fd, cb) ->
    # logger.log "debug", "writing to file #{path} - position: #{position}, length: #{len}"
    file = folderTree.get path  
    size = file.size
    fs.write fd, buf, 0, len, position, (err, bytesWritten, buffer) ->
      if (err)
        logger.debug "there was an error writing to the #{path}, #{file}"
        logger.debug err
        logger.debug err.code
        cb(-errnoMap[err.code])
        return

      #it is simportant to update the file size as we copy in to it. sometimes, cp and mv will check the progress by scanning the filesystem
      if size < (position + len)
        file.size = position + len
      cb(bytesWritten)
      return
    return

  flush: (context, inode, fileInfo, reply) ->
      console.log('Flush was called!');
      reply.err(0)
      return

  # /*
  #  * Handler for the mkdir() system call.
  #  * path: the path of the new directory
  #  * mode: the desired permissions of the new directory
  #  * cb: a callback of the form cb(err), where err is the Posix return code.
  #  */
  mkdir: (path, mode, cb) ->
    logger.debug "creating folder #{path}"
    parent = folderTree.get pth.dirname(path)
    if parent #make sure that the parent exists
      if parent instanceof GFolder #make sure that the parent is a folder
        name = pth.basename path
        unless parent.children.indexOf name < 0 #make sure that the child doesn't already exist
          console.log parent.children
          cb(-errnoMap.EEXIST)
          return
        folder =
          resource:
            title: name
            mimeType: 'application/vnd.google-apps.folder'
            parents: [{id: parent.id}]

        drive.files.insert folder, (err, res) ->
          if err
            logger.log "error", err
            cb(-errnoMap.EIO)
            return
          else
            parent.children.push name
            folderTree.set path, new GFolder(res.id, res.parents[0].id, name, (new Date(res.createdDate)).getTime(), (new Date(res.modifiedDate)).getTime(), res.editable, [])
            client.idToPath.set(res.id, path)
            cb(0)
            client.saveFolderTree()
          return
      else
        cb(-errnoMap.ENOTDIR)
    else
      cb(-errnoMap.ENOENT)
    return

  # /*
  #  * Handler for the rmdir() system call.
  #  * path: the path of the directory to remove
  #  * cb: a callback of the form cb(err), where err is the Posix return code.
  #  */
  rmdir: (path, cb) ->
    logger.log "debug", "removing folder #{path}"
    if folderTree.has path #make sure that the path exists
      folder = folderTree.get path
      if folder instanceof GFolder #make sure that the folder is in fact a folder
        if folder.children.length == 0
          drive.files.trash {fileId: folder.id}, (err, res) ->
            if err
              logger.log "error", "unable to remove folder #{path}"
              cb -errnoMap.EIO
              return
            else
              console.log res
              parent = folderTree.get pth.dirname(path)
              name = pth.basename path
              idx = parent.children.indexOf name
              if idx >= 0
                parent.children.splice idx, 1
              folderTree.remove path
              client.idToPath.remove(folder.id)

              cb(0)
              client.saveFolderTree()
              return
            return  
        else
          cb -errnoMap.ENOTEMPTY
          return
      else
        cb -errnoMap.ENOTDIR
        return

    else
      cb -errnoMap.ENOENT
      return
    return


   #  /*
   # * Handler for the create() system call.
   # * path: the path of the new file
   # * mode: the desired permissions of the new file
   # * cb: a callback of the form cb(err, [fh]), where err is the Posix return code
   # *     and fh is an optional numerical file handle, which is passed to subsequent
   # *     read(), write(), and release() calls (it's set to 0 if fh is unspecified)
   # */
  create: (path, mode, cb) ->
    cache = MD5(path)
    systemPath = pth.join(uploadLocation, cache);
    logger.log "debug", "creating file"

    parentPath = pth.dirname path
    parent = folderTree.get parentPath
    if parent #make sure parent exists
      name = pth.basename path

      if parent.children.indexOf(name) < 0 #TODO: if file exists, delete it first
        parent.children.push name
      now = (new Date).getTime()
      logger.log "debug", "adding #{path} to folderTree"
      folderTree.set path, new GFile(null, null, parent.id, name, 0, now, now, true)
      client.saveFolderTree()


      fs.open systemPath, 'w', (err, fd) ->
        if (err)
          logger.log "error", "unable to createfile #{path}, #{err}"
          cb(-errnoMap[err.code])
          return

        logger.log "debug", "setting upload Tree"
        upFile = 
          cache: cache
          uploading: false
        uploadTree.set path, upFile
        saveUploadTree()
        cb(0, fd);
        return
    else
      cb( -errnoMap.ENOENT )
    return
  # /*
  #  * Handler for the unlink() system call.
  #  * path: the path to the file
  #  * cb: a callback of the form cb(err), where err is the Posix return code.
  #  */
  unlink: (path, cb) ->
    logger.log "debug", "removing file #{path}"
    if folderTree.has path #make sure that the path exists
      file = folderTree.get path
      if file instanceof GFile #make sure that the folder is in fact a folder
        drive.files.trash {fileId: file.id}, (err, res) ->
          if err
            logger.log "error", "unable to remove file #{path}"
          parent = folderTree.get pth.dirname(path)
          name = pth.basename path
          idx = parent.children.indexOf name
          if idx >= 0
            parent.children.splice idx, 1
          folderTree.remove path
          client.saveFolderTree()
          client.idToPath.remove(file.id)

          cb(0) #always return success
          return          
      else
        cb -errnoMap.EISDIR    
    else
      cb -errnoMap.EEXIST
    return


  # /*
  #  * Handler for the release() system call.
  #  * path: the path to the file
  #  * fd:  the optional file handle originally returned by open(), or 0 if it wasn't
  #  * cb: a callback of the form cb(err), where err is the Posix return code.
  #  */
  release: (context, inode, fileInfo, reply) ->
    console.log('Release was called!');
    reply.err(0);
    return
    logger.silly "closing file #{path}"
    if uploadTree.has path
      logger.log "debug", "#{path} was in the upload tree"
      client.saveFolderTree()
      #close the file
      fs.close fd, (err) ->
        if (err)
          cb(-errnoMap[err.code])
          return
        cb(0)
        #upload file once file is closed
        parent = folderTree.get pth.dirname(path)

        if folderTree.has path
          file = folderTree.get(path)
          ###
          three cases: 
          if file size is 0: delete it and don't upload
          if file size is <=10MB, just upload it directly
          if file size is >10 MB, add to upload queue
          ###

          if 0 < file.size <=  10485760 #10MB 
            cb = ->
              return
            parent.upload pth.basename(path), path, uploadCallback(path, cb)           
          else if file.size >  10485760 
            fn = (cb)->
              parent.upload pth.basename(path), path, uploadCallback(path,cb)            
              return
            q.push fn
            q.start()
          else          
            uploadTree.remove path
            saveUploadTree()      

          return
        console.log "no uploading"
        return

    else
      cb(0)
    return

  statfs: (context, inode, reply) ->
    reply.statfs {
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
          flag: 0,
      }

  getxattr: (context, inode, name, size, position, reply) ->
      console.log('GetXAttr was called!')
      parentPath = inodeToPath.get inode
      childPath = pth.join(parentPath, name)
      if folderTree.has childPath
        reply.err 0
      else
        reply.err PosixError.ENOENT
  access: (context, inode, mask, reply) ->
      console.log('Access was called!');
      reply.err(0);

  lookup: (context, parent, name, reply) ->
      parentPath = inodeToPath.get parent
      # parent = folderTree.get parentPath
      childPath =  pth.join(parentPath, name)
      parent = folderTree.get(parentPath)
      child = folderTree.get childPath
      if folderTree.has childPath
        child = folderTree.get childPath
        attr = child.getAttrSync()
        attr.size ||= 4096
        entry = {
            inode: attr.inode,
            # generation: 2,
            attr: attr,
              # {
                # # dev: 234881026,
                # # ino: 13420595,
                # mode: 33188,
                # nlink: 1,
                # uid: context.uid,
                # gid: context.gid,
                # rdev: 0,
                # size: child.size || 4096,
                # # blksize: 4096,
                # # blocks: 8,
                # atime: 1331780451475, 
                # mtime: 1331780451475, 
                # ctime: 1331780451475, 
            # },
            attr_timeout: 30,
            entry_timeout: 60
        }
        reply.entry(entry)
      else
        reply.err PosixError.ENOENT
      return



    #recursively read and write streams
moveToDownload = (file, fd, uploadedFileLocation, start,cb) ->

  end = Math.min(start + GFile.chunkSize, file.size)-1
  savePath = pth.join(config.cacheLocation, 'download', "#{file.id}-#{start}-#{end}");
  rstream = fs.createReadStream(uploadedFileLocation, {fd: fd, autoClose: false, start: start, end: end})
  wstream = fs.createWriteStream(savePath)
  rstream.pipe(wstream)

  rstream.on 'end',  ->        
    start += GFile.chunkSize

    if start < file.size
      moveToDownload(file, fd, uploadedFileLocation, start, cb)
    else
      fs.close fd, (err) ->
        if err
          logger.debug "unable to close file after transffering #{uploadedFile}"
          cb()
        else
          fs.unlink uploadedFileLocation, (err)->
            if err
              logger.log "error", "unable to remove file #{uploadedFile}"      
            cb()
            return
        return
    return

  return

#function to create a callback for file uploading
uploadCallback = (path, cb) ->
  return (err, result) ->
    parent = folderTree.get pth.dirname(path)
    if err
      if err == "invalid mime"
        logger.debug "the mimetype of #{path} was invalid"
        cb()
        return
      logger.debug "failed to upload \"#{path}\". Retrying"
      fn = (cb) ->
        parent.upload pth.basename(path), path , callback(path,cb)
        return
      cb()
      q.push fn
      q.start()
      return
    else
      uploadedFile = uploadTree.get(path)
      uploadedFileLocation = pth.join uploadLocation, uploadedFile.cache

      logger.log 'info', "successfully uploaded #{path}"
          
      uploadTree.remove path
      saveUploadTree()
      if folderTree.has path
        logger.debug "#{path} folderTree already existed"
        file = folderTree.get path
        file.downloadUrl = result.downloadUrl
        file.id = result.id
        file.size = parseInt(result.fileSize)
        file.ctime = (new Date(result.createdDate)).getTime()
        file.mtime =  (new Date(result.modifiedDate)).getTime()
      else
        logger.debug "#{path} folderTree did not exist"        
        file = new GFile(result.downloadUrl, result.id, result.parents[0].id, result.title, parseInt(result.fileSize), (new Date(result.createdDate)).getTime(), (new Date(result.modifiedDate)).getTime(), true)        

      client.idToPath.set( result.id, path)
      #update folder Tree
      if parent.children.indexOf( file.name ) < 0
        parent.children.push file.name
      folderTree.set path, file
      client.saveFolderTree()

      #move the file to download folder after finished uploading
      fs.open uploadedFileLocation, 'r', (err,fd) ->
        if err
          logger.debug "could not open #{uploadedFileLocation} for copying file from upload to uploader"
          logger.debug err
          return
        else          
          moveToDownload(file, fd, uploadedFileLocation, 0, cb)
        return

    return

#resume file uploading
fn = ->
  q.start()
  if uploadTree.count() > 0
    logger.info "resuming file uploading"
    for path in uploadTree.keys()
        if folderTree.has pth.dirname(path)
          parent = folderTree.get pth.dirname(path)
          _fn = (cb) ->
            parent.upload pth.basename(path), path, uploadCallback(path,cb)
            return
          q.push(_fn)
          q.start()
  return
setTimeout fn, 25000

start = ->
  if folderTree.count() > 1
    try
      logger.log "info", 'attempting to start f4js'
      switch os.type()
        when 'Linux' 
          opts = []
          command = "fusermount -u #{config.mountPoint}"          
        when 'Darwin'
          opts = ["-o",'daemon_timeout=0', "-o", "noappledouble", "-o", "noubc", "-o", "default_permissions"]
          command = "diskutil umount force #{config.mountPoint}"
        else 
          opts = []
          command = "fusermount -u #{config.mountPoint}"
      if process.version < '0.11.0'
        opts.push( "-o", "allow_other")

      fs.ensureDirSync(config.mountPoint)
      debug = false

      exec command, (err, data) ->
        if err
          logger.error "unmount error:", err
        if data
          logger.info "unmounting output:", data
        opts =  ["GDrive", "-s", "-f", "-o", "allow_other", config.mountPoint]
        # opts.push "-s"
        # opts.push "-f"

        # opts.push "-mt"
        # opts.push "-d"
        fuse.fuse.mount
          filesystem: GDriveFS
          options: opts
        logger.log('info', "mount point: #{config.mountPoint}")
        return
    catch e
      logger.log( "error", "Exception when starting file system: #{e}")
  else
    setTimeout start, 500
  return
  
start()  
