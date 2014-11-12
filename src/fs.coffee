google = require 'googleapis'
fs = require 'fs-extra'
winston = require 'winston'
rest = require 'restler'
hashmap = require( 'hashmap' ).HashMap
NodeCache = require 'node-cache'
pth = require 'path'
f4js = require 'fuse4js'
os = require 'os'

client = require('./client.coffee')
folderTree = client.folderTree
drive = client.drive
GFolder = require("./folder.coffee").GFolder
GFile = require("./file.coffee").GFile

logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'info' }),
      new (winston.transports.File)({ filename: '/tmp/GDriveF4JS.log', level:'debug' })
    ]
  })

#read input config
config = fs.readJSONSync 'config.json'
GFile.chunkSize = config.chunkSize

#http://lxr.free-electrons.com/source/include/uapi/asm-generic/errno-base.h#L23
errnoMap =
    EPERM: 1,
    ENOENT: 2,
    EIO: 5,
    EACCES: 13,
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
  console.log "opening file #{path} - flags: #{flags}/#{flag}"
  switch flag
    when 0 #read only
      if folderTree.has(path)
        cb(0,null)# // we don't return a file handle, so fuse4js will initialize it to 0
        return null
      else
        cb(-errnoMap.ENOENT)
        return null

    when 1 #write only
      logger.log 'info', "tried to open file \"#{path}\" for writing"
      cb(-errnoMap.ENOENT)

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



flush = (buf, cb) ->
  return cb(0)
release =  (path, fh, cb) ->
  return cb(0)

# /*
#  * Handler for the mkdir() system call.
#  * path: the path of the new directory
#  * mode: the desired permissions of the new directory
#  * cb: a callback of the form cb(err), where err is the Posix return code.
#  */
mkdir = (path, mode, cb) ->
  parent = folderTree.get pth.dirname(path)
  folder =
    resource:
      title: pth.basename path
      mimeType: 'application/vnd.google-apps.folder'
      parents: [{id: parent.id}]

  drive.files.insert folder, (err, res) ->
    if err
      logger.log "error", err
      return cb(-errno.EIO)
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


handlers =
  readdir:    readdir
  statfs:     statfs
  getattr:    getattr
  open:       open
  read:       read
  flush:      flush
  release:    release
  mkdir:      mkdir

try
  logger.log "info", 'attempting to start f4js'
  opts = switch os.type()
    when 'Linux' then  ["-o", "allow_other"]
    when 'Darwin' then  ["-o", "allow_other", "-o", "noappledouble", "-o", "daemon_timeout=0", '-o', 'noubc']
    else []
  fs.ensureDirSync(config.mountPoint)
  f4js.start(config.mountPoint, handlers, false, opts);
  logger.log('info', "mount point: #{config.mountPoint}")
catch e
  logger.log( "error", "Exception when starting file system: #{e}")
