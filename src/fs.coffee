google = require 'googleapis'
fs = require 'fs-extra'
winston = require 'winston'
rest = require 'restler'
hashmap = require( 'hashmap' ).HashMap
NodeCache = require 'node-cache'
pth = require 'path'
f4js = require 'fuse4js'
os = require 'os'
MD5 = require 'MD5'

client = require('./client.coffee')
folderTree = client.folderTree
drive = client.drive
GFolder = require("./folder.coffee").GFolder
GFile = require("./file.coffee").GFile
uploadTree = require("./folder.coffee").uploadTree
logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'debug' }),
      new (winston.transports.File)({ filename: '/tmp/GDriveF4JS.log', level:'debug' })
    ]
  })

#read input config
config = fs.readJSONSync 'config.json'
GFile.chunkSize = config.chunkSize
GFolder.uploadChunkSize = config.uploadChunkSize
uploadLocation = pth.join config.cacheLocation, 'upload'


#http://lxr.free-electrons.com/source/include/uapi/asm-generic/errno-base.h#L23
errnoMap =
    EPERM: 1,
    ENOENT: 2,
    EIO: 5,
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


getattr = (path, cb) ->
  if folderTree.has(path)
    callback = (status, attr)->
      cb(status, attr)
    return folderTree.get(path).getAttr(callback)
  else
    return cb(-errnoMap.ENOENT)


# /*
#  * Handler for the readdir() system call.
#  * path: the path to the file
#  * cb: a callback of the form cb(err, names), where err is the Posix return code
#  *     and names is the result in the form of an array of file names (when err === 0).
#  */
readdir = (path, cb) ->
  names = []
  if folderTree.has(path)
    object = folderTree.get(path)
    if object instanceof GFile
      err = -errnoMap.ENOTDIR
    else if object instanceof GFolder
      err = 0
      names = object.children
    else
      err = -errnoMap.ENOENT
  else
    err = -errnoMap.ENOENT
  return cb( err, names );

open = (path, flags, cb) ->
  err = 0 # assume success
  flag = flags & 3
  logger.log "debug", "opening file #{path} - flags: #{flags}/#{flag}"
  parent = folderTree.get pth.dirname(path)
  switch flag
    when 0 #read only
      if folderTree.has(path)
        file = folderTree.get(path)
        if file instanceof GFile
          if file.downloadUrl #make sure that the file has been fully uploaded
            cb(0,null)# // we don't return a file handle, so fuse4js will initialize it to 0
          else
            cb -errnoMap.EACCESS
        else
          cb -errnoMap.EISDIR
        return null
      else
        cb(-errnoMap.ENOENT)
        return null

    when 1 #write only
      logger.log 'debug', "tried to open file \"#{path}\" for writing"
      if folderTree.has(path) #if folderTree has path, make sure it's a file with size zero
        file = folderTree.get(path)
        if (file instanceof GFile)
           if file.size == 0
            cb 0, null
           else
             cb -errnoMap.ENOENT
        else
          cb -errnoMap.EISDIR
        return null
      else #if it doesn't have the path, create the file
        parent = folderTree.get pth.dirname(path)
        if parent and parent instanceof GFolder
          now = ( new Date()).getTime()
          name = pth.basename(path)

          file = new GFile(null, null, parent.id, name, 0, now, now, true)
          cache = MD5(path)
          folderTree.set path, file
          uploadTree.set path, cache

          if parent.children.indexOf(name) < 0
            parent.children.push name

          fs.open pth.join(uploadLocation, cache), 'w', (err,fd) ->
            if err
              cb -errnoMap[err.code]
            else
              cb 0, fd

          return null
        else
          cb -errnoMap.EPERM
          return null



        if folderTree.get(path) instanceof GFile
          cb(0,null)# // we don't return a file handle, so fuse4js will initialize it to 0
        else
          cb -errnoMap.EISDIR
        return null



      cb(-errnoMap.ENOENT)
      return null

    when 2 #read/write
      logger.log 'info', "tried to open file \"#{path}\" for r+w"
      cb(-errnoMap.ENOENT)

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
read = (path, offset, len, buf, fh, cb) ->
  logger.log "silly", "reading file #{path} - #{offset}:#{len}"

  if folderTree.has(path)
    callback = (dataBuf) ->
      try
        dataBuf.copy(buf)
        cb(dataBuf.length)
        return null
      catch error
        logger.log( "error", "failed reading: #{error}")
        cb(-errnoMap.EIO)

    #make sure that we are only reading a file
    file = folderTree.get(path)
    if file instanceof GFile

      #make sure the offset request is not bigger than the file itself
      if offset < file.size
        file.download(offset, offset+len-1,true,callback)
      else
        cb(-errnoMap.ESPIPE)
    else
      cb(-errnoMap.EISDIR)

  else
    return cb(-errnoMap.ENOENT)

# /*
#  * Handler for the write() system call.
#  * path: the path to the file
#  * offset: the file offset to write to
#  * len: the number of bytes to write
#  * buf: the Buffer to read data from
#  * fd:  the optional file handle originally returned by open(), or 0 if it wasn't
#  * cb: a callback of the form cb(err), where err is the Posix return code.
#  *     A positive value represents the number of bytes actually written.
#  */
write = (path, offset, len, buf, fd, cb) ->
  logger.log "debug", "writing to file #{path} - offset: #{offset}, length: #{len}"
  fs.write fd, buf, 0, len, offset, (err, bytesWritten, buffer) ->
    if (err)
      return cb(-errnoMap[err.code])
    cb(bytesWritten)

# flush = (buf, cb) ->
#   return cb(0)

# /*
#  * Handler for the mkdir() system call.
#  * path: the path of the new directory
#  * mode: the desired permissions of the new directory
#  * cb: a callback of the form cb(err), where err is the Posix return code.
#  */
mkdir = (path, mode, cb) ->
  parent = folderTree.get pth.dirname(path)
  if parent #make sure that the parent exists
    if parent instanceof GFolder #make sure that the parent is a folder
      name = pth.basename path
      unless parent.children.indexOf name < 0 #make sure that the child doesn't already exist
        console.log parent.children
        cb(-errnoMap.EEXIST)
        return null
      folder =
        resource:
          title: name
          mimeType: 'application/vnd.google-apps.folder'
          parents: [{id: parent.id}]

      drive.files.insert folder, (err, res) ->
        if err
          logger.log "error", err
          return cb(-errnoMap.EIO)
        else
          parent.children.push name
          folderTree.set path, new GFolder(res.id, res.parents[0].id, name, (new Date(res.createdDate)).getTime(), (new Date(res.modifiedDate)).getTime(), res.editable, [])
          cb(0)
          client.saveFolderTree()
          return null
    else
      cb(-errnoMap.ENOTDIR)
  else
    cb(-errnoMap.ENOENT)
    return null

# /*
#  * Handler for the rmdir() system call.
#  * path: the path of the directory to remove
#  * cb: a callback of the form cb(err), where err is the Posix return code.
#  */
rmdir = (path, cb) ->
  logger.log "debug", "removing folder #{path}"
  if folderTree.has path #make sure that the path exists
    folder = folderTree.get path
    if folder instanceof GFolder #make sure that the folder is in fact a folder
      if folder.children.length == 0
        drive.files.trash {fileId: folder.id}, (err, res) ->
          if err
            logger.log "error", "unable to remove folder #{path}"
            cb -errnoMap.EIO
          else
            console.log res
            parent = folderTree.get pth.dirname(path)
            name = pth.basename path
            idx = parent.children.indexOf name
            if idx >= 0
              parent.children.splice idx, 1
            folderTree.remove path
            client.saveFolderTree()
            cb 0
      else
        cb -errnoMap.ENOTEMPTY
    else
      cb -errnoMap.ENOTDIR

  else
    cb -errnoMap.EEXIST
    return null



 #  /*
 # * Handler for the create() system call.
 # * path: the path of the new file
 # * mode: the desired permissions of the new file
 # * cb: a callback of the form cb(err, [fh]), where err is the Posix return code
 # *     and fh is an optional numerical file handle, which is passed to subsequent
 # *     read(), write(), and release() calls (it's set to 0 if fh is unspecified)
 # */
create = (path, mode, cb) ->
  cache = MD5(path)
  systemPath = pth.join(uploadLocation, cache);
  logger.log "debug", "creating file"

  parentPath = pth.dirname path
  parent = folderTree.get parentPath
  if parent #make sure parent exists
    name = pth.basename path

    unless parent.children.indexOf(name) < 0 #make sure file doesn't exist yet
      cb(-errnoMap.EEXIST)
      return null
    parent.children.push name
    fs.open systemPath, 'w', mode, (err, fd) ->
      if (err)
        logger.log "error", "unable to createfile #{path}, #{err}"
        return cb(-errnoMap[err.code])

      logger.log "debug", "setting upload Tree"
      uploadTree.set path, cache

      now = (new Date).getTime()
      logger.log "debug", "adding #{path} to folderTree"
      folderTree.set path, new GFile(null, null, parent.id, name, 0, now, now, true)
      cb(0, fd);
      return null
  else
    cb( -errnoMap.ENOENT )
    return null

# /*
#  * Handler for the unlink() system call.
#  * path: the path to the file
#  * cb: a callback of the form cb(err), where err is the Posix return code.
#  */
unlink = (path, cb) ->
  logger.log "debug", "removing file #{path}"
  if folderTree.has path #make sure that the path exists
    file = folderTree.get path
    if file instanceof GFile #make sure that the folder is in fact a folder
      drive.files.trash {fileId: file.id}, (err, res) ->
        if err
          logger.log "error", "unable to remove folder #{path}"
          cb -errnoMap.EIO
        else
          console.log res
          parent = folderTree.get pth.dirname(path)
          name = pth.basename path
          idx = parent.children.indexOf name
          if idx >= 0
            parent.children.splice idx, 1
          folderTree.remove path
          client.saveFolderTree()
          cb 0
    else
      cb -errnoMap.EISDIR

  else
    cb -errnoMap.EEXIST
    return null



# /*
#  * Handler for the release() system call.
#  * path: the path to the file
#  * fd:  the optional file handle originally returned by open(), or 0 if it wasn't
#  * cb: a callback of the form cb(err), where err is the Posix return code.
#  */
release = (path, fd, cb) ->
  logger.log "debug", "closing file #{path}"
  if uploadTree.has path
    logger.log "debug", "#{path} was in the upload tree"

    #close the file
    fs.close fd, (err) ->
      if (err)
        cb(-errnoMap[err.code])
        return null
      cb(0)
      #upload file once file is closed
      parent = folderTree.get pth.dirname(path)
      callback = (err, result) ->
        if err
          logger.log "error", "failed to upload \"#{path}\". Retrying"
          parent.upload uploadTree.get(path), callback
        else
          logger.log 'info', "successfully uploaded #{path}"
          uploadTree.remove path
          file = folderTree.get path
          file.downloadUrl = result.downloadUrl
          file.id = result.id
          file.size = parseInt(result.fileSize)
          file.ctime = (new Date(file.createdDate)).getTime()
          file.mtime =  (new Date(file.modifiedDate)).getTime()
          client.saveFolderTree()
      parent.upload pth.basename(path), pth.join(uploadLocation, uploadTree.get(path)), callback
      return null

  else
    cb(0)
    return null

statfs= (cb) ->
  return cb(0, {
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
        namemax: 64
    })

flush = (cb) ->
  cb(0)
  return null

handlers =
  readdir:    readdir
  statfs:     statfs
  getattr:    getattr
  open:       open
  read:       read
  flush:      flush
  release:    release
  mkdir:      mkdir
  create:     create
  write:      write
  unlink:     unlink
  rmdir:      rmdir

try
  logger.log "info", 'attempting to start f4js'
  opts = switch os.type()
    when 'Linux' then  ["-o", "allow_other"]
    when 'Darwin' then  ["-o", "allow_other", "-o", "noappledouble", "-o", "daemon_timeout=0", '-o', 'noubc']
    else []
  fs.ensureDirSync(config.mountPoint)
  debug = false
  f4js.start(config.mountPoint, handlers, debug, opts);
  logger.log('info', "mount point: #{config.mountPoint}")
catch e
  logger.log( "error", "Exception when starting file system: #{e}")
