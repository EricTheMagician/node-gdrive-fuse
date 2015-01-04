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
GFile.chunkSize =  config.chunkSize || GFile.chunkSize #8388608 #MB default
GFile.GDrive = client.drive;
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
    logger.silly "getting attr for #{path}"
    if folderTree.has(path)
      callback = (status, attr)->
        reply.attr(attr, 5)
        return
      folderTree.get(path).getAttr(callback)
        
    else
      reply.err(errnoMap.ENOENT)
    return

  opendir: (context, inode, fileInfo, reply) ->
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
    logger.silly "readding dir #{path}"
    # console.log path
    if folderTree.has(path)
      object = folderTree.get(path)
      if object instanceof GFile
        reply.err errnoMap.ENOTDIR
      else if object instanceof GFolder
        size = Math.max( requestedSize , object.children.length * 256)
        # size = requestedSize
        parent = folderTree.get pth.dirname(path)
        totalSize = 0
        #totalSize += reply.addDirEntry('.', requestedSize, {inode: object.inode}, offset);
        # totalSize += reply.addDirEntry('..', requestedSize, {inode: parent.inode}, offset);
        for child in object.children
          cpath = pth.join(path,child)
          cnode = folderTree.get cpath
          attr = cnode.getAttrSync()
          len = reply.addDirEntry(child, size, {inode: cnode.inode}, offset);          
          totalSize += len

        if object.children.length == 0
          reply.buffer(new Buffer(0), 0)
        else
          reply.buffer(new Buffer(0), requestedSize)
      else
        reply.err errnoMap.ENOENT
    else
      reply.err errnoMap.ENOENT
    return

  setattr: (context, inode, attrs, reply) ->
    path = inodeToPath.get inode
    logger.silly "setting attr for #{path}"
    file = folderTree.get path
    if 'size' in attrs
      file.size = attrs.size

    reply.attr(file.getAttrSync(), 5);
    return

  open: (context, inode, fileInfo, reply) ->
    path = inodeToPath.get inode
    logger.silly "opening file #{path}"
    parent = folderTree.get pth.dirname(path)
    flags = fileInfo.flags
    if flags.rdonly #read only
      if folderTree.has(path)
        file = folderTree.get(path)
        if file instanceof GFile
          if file.downloadUrl #make sure that the file has been fully uploaded
            reply.open(fileInfo)
          # else if uploadTree.has(path) #after writing a file, sometimes the filesystem tries to open the file again.
          #   fs.open pth.join(uploadLocation, uploadTree.get(path).cache), 'r', (err,fd) ->
          #     if err
          #       reply.err errnoMap.EACCESS
          #       return
          #     fileInfo.fh = fd
          #     reply.open fileInfo
          #     return
          else
            reply.err errnoMap.EACCESS
        else
          reply.errerrnoMap.EISDIR
      else
        reply.err errnoMap.ENOENT
      return

    if flags.wronly #write only
      cache = MD5(path)
      logger.log 'debug', "tried to open file \"#{path}\" for writing"
      if folderTree.has(path) #if folderTree has path, make sure it's a file with size zero
        file = folderTree.get(path)
        if (file instanceof GFile)
         if file.size == 0
          # logger.debug "#{path} size was 0"
          fs.open pth.join(uploadLocation, cache), 'w+', (err,fd) ->
            if err
              logger.debug "could not open file for writing"
              logger.debug err
              reply.err errnoMap[err.code]
              return

            fileInfo.fh = fd
            reply.open(fileInfo)
            return

         else
           reply.err errnoMap.EACCESS
        else
          reply.err errnoMap.EISDIR
        return
      if flags.rdwr #if it doesn't have the path, create the file
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
      # logger.log 'info', "tried to open file \"#{path}\" for r+w"
      reply.err errnoMap.ENOENT

    return

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
          reply.err(errnoMap.ESPIPE)
      else
        reply.err(errnoMap.EISDIR)

    else
      reply.err(errnoMap.ENOENT)



    return

  write: (context, inode, buffer, position, fileInfo, reply) ->

    path = inodeToPath.get inode
    logger.log "silly", "writing to file #{path} - position: #{position}, length: #{buffer.length}"

    file = folderTree.get path  
    size = file.size
    fs.write fileInfo.fh, buffer, 0, buffer.length, position, (err, bytesWritten, buffer) ->
      if (err)
        logger.debug "there was an error writing to #{path}"
        logger.debug err
        logger.debug "position", position, "fh", fileInfo.fh
        reply.err(err.errno)
        return

      #it is simportant to update the file size as we copy in to it. sometimes, cp and mv will check the progress by scanning the filesystem
      if size < (position + buffer.length)
        file.size = position + buffer.length
      reply.write(bytesWritten)
      return
    return

  flush: (context, inode, fileInfo, reply) ->
      reply.err(0)
      return

  # /*
  #  * Handler for the mkdir() system call.
  #  * path: the path of the new directory
  #  * mode: the desired permissions of the new directory
  #  * cb: a callback of the form cb(err), where err is the Posix return code.
  #  */
  mkdir: (context, parentInode, name, mode, reply) ->
    parentPath = inodeToPath.get parentInode
    path = pth.join parentPath, name
    logger.debug "creating folder #{path}"

    parent = folderTree.get parentPath
    if parent #make sure that the parent exists
      if parent instanceof GFolder #make sure that the parent is a folder
        unless parent.children.indexOf name < 0 #make sure that the child doesn't already exist
          reply.err errnoMap.EEXIST
          return
        folder =
          resource:
            title: name
            mimeType: 'application/vnd.google-apps.folder'
            parents: [{id: parent.id}]

        drive.files.insert folder, (err, res) ->
          if err
            logger.log "error", err
            reply.err(errnoMap.EIO)
            return
          else
            parent.children.push name
            inodes = value.inode for value in folderTree.values()
            inode = Math.max(inodes) + 1
            folder = new GFolder(res.id, res.parents[0].id, name, (new Date(res.createdDate)).getTime(), (new Date(res.modifiedDate)).getTime(), inode, res.editable, [])
            folderTree.set path, folder
            inodeToPath.set inode, path 
            # idToPath.set res.id, path
            client.idToPath.set(res.id, path)
            attr = folder.getAttrSync()
            entry = {
                inode: attr.inode,
                generation: 2,
                attr: attr,
                attr_timeout: 5,
                entry_timeout: 5
            }
            reply.entry(entry)
            client.saveFolderTree()
          return
      else
        reply.err(errnoMap.ENOTDIR)
    else
      reply.err(errnoMap.ENOENT)
    return

  # /*
  #  * Handler for the rmdir() system call.
  #  * path: the path of the directory to remove
  #  * cb: a callback of the form cb(err), where err is the Posix return code.
  #  */
  rmdir: (context, parentInode, name, reply) ->
    parentPath = inodeToPath.get parentInode
    path = pth.join parentPath, name 
    logger.log "debug", "removing folder #{path}"
    if folderTree.has path #make sure that the path exists
      folder = folderTree.get path
      if folder instanceof GFolder #make sure that the folder is in fact a folder
        if folder.children.length == 0
          drive.files.trash {fileId: folder.id}, (err, res) ->
            if err
              logger.log "error", "unable to remove folder #{path}"
              reply.err errnoMap.EIO
              return
            else
              parent = folderTree.get pth.dirname(path)
              name = pth.basename path
              idx = parent.children.indexOf name
              if idx >= 0
                parent.children.splice idx, 1
              folderTree.remove path
              client.idToPath.remove(folder.id)

              reply.err 0
              client.saveFolderTree()
              return
            return  
        else
          reply.err errnoMap.ENOTEMPTY
          return
      else
        reply.err errnoMap.ENOTDIR
        return

    else
      reply.err errnoMap.ENOENT
      return
    return

  mknod: (context, parentInode, name, mode, rdev, reply) ->
    parentPath = inodeToPath.get parentInode
    parent = folderTree.get parentPath
    logger.log "debug", "adding #{name} to #{parentPath}, #{parentInode}"
    path = pth.join parentPath, name

    if folderTree.has(path)
      reply.err PosixError.EEXIST
      return
      
    now = (new Date).getTime()

    inodes = value.inode for value in folderTree.values()
    inode = Math.max(inodes) + 1
    inodeToPath.set inode, path

    file = new GFile(null, null, parent.id, name, 0, now, now, inode, true)
    folderTree.set path, file

    if parent.children.indexOf(name) < 0 #TODO: if file exists, delete it first

      parent.children.push name

    attr = file.getAttrSync()

    upFile = 
      cache: MD5(path)
      uploading: false
    uploadTree.set path, upFile
    saveUploadTree()


    entry = 
        inode: attr.inode
        generation: 2
        attr: attr
        # attr_timeout: 30,
        # entry_timeout: 60
    
    reply.entry(entry)
    return


  create: (context, parentInode, name, mode, fileInfo, reply) ->
    parentPath = inodeToPath.get parentInode
    path = pth.join parentPath, name
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
      inodes = value.inode for value in folderTree.values()
      inode = Math.max(inodes) + 1
      file = new GFile(null, null, parent.id, name, 0, now, now, inode, true)
      folderTree.set path, file
      inodeToPath.set inode, path

      client.saveFolderTree()


      fs.open systemPath, 'w', (err, fd) ->
        if (err)
          logger.log "error", "unable to createfile #{path}, #{err}"
          reply.err(errnoMap[err.code])
          return
        fileInfo.fh = fd
        logger.log "debug", "setting upload Tree"
        upFile = 
          cache: cache
          uploading: false
        uploadTree.set path, upFile
        saveUploadTree()
        attr = 
          inode: inode #parent.inode
          generation: 1
          attr:file
        reply.create attr, fileInfo
        return
    else
      reply.err( errnoMap.ENOENT )
    return
  # /*
  #  * Handler for the unlink() system call.
  #  * path: the path to the file
  #  * cb: a callback of the form cb(err), where err is the Posix return code.
  #  */
  unlink: (context, parentInode, name, reply) ->
    parentPath = inodeToPath.get parentInode
    path = pth.join parentPath, name

    logger.log "debug", "removing file #{path}, name is #{name}-#{folderTree.has path}"
    if folderTree.has path #make sure that the path exists
      file = folderTree.get path
      if file instanceof GFile #make sure that the file is in fact a file
        
        folderTree.remove path
        drive.files.trash {fileId: file.id}, (err, res) ->
          if err
            logger.log "debug", "unable to remove file #{path}"
          parent = folderTree.get pth.dirname(path)
          name = pth.basename path
          idx = parent.children.indexOf name
          if idx >= 0
            parent.children.splice idx, 1
          client.saveFolderTree()
          client.idToPath.remove(file.id)

          reply.err 0 #always return success
          return          
      
        #check if file was being uploaded
        if uploadTree.has path
          cache = uploadTree.get(path).cache
          uploadTree.remove path
          fs.unlink pth.join(uploadLocation,cache), (err)->
          

      else
        reply.err errnoMap.EISDIR    
    else
      reply.err errnoMap.ENOENT
    return


  # /*
  #  * Handler for the release() system call.
  #  * path: the path to the file
  #  * fd:  the optional file handle originally returned by open(), or 0 if it wasn't
  #  * cb: a callback of the form cb(err), where err is the Posix return code.
  #  */
  release: (context, inode, fileInfo, reply) ->
    path = inodeToPath.get inode
    logger.silly "closing file #{path}"
    if uploadTree.has path
      logger.log "debug", "#{path} was in the upload tree"
      client.saveFolderTree()
      #close the file
      fs.close fileInfo.fh, (err) ->
        if (err)
          reply.err err.errno
          return
        reply.err(0)
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
              if parent instanceof GFile
                console.log "While uploading, #{pth.dirname(path)} was a file - #{parent}"
                cb()
                return
              parent.upload pth.basename(path), path, uploadCallback(path,cb)            
              return
            q.push fn
            q.start()
          else          
            uploadTree.remove path
            saveUploadTree()      

          return
        return

    else if fileInfo.fh
      fs.close fileInfo.fh, (err) ->
        if err
          reply.err err.errno
        reply.err 0
    else    
      reply.err(0)
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
    return

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
        entry = 
            inode: attr.inode
            generation: 2
            attr: attr
            # attr_timeout: 5,
            # entry_timeout: 5
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
      return
    fs.close fd, (err) ->
      if err
        logger.debug "unable to close file after transffering #{uploadedFile}"
        cb()
        return        
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
      if err == "uploading"
        cb()
        return
      if err.code == "ENOENT"
        uploadTree.remove(path)
        cb()
        return

      cb()
      logger.debug "Retrying upload: \"#{path}\"."
      fn = (cb) ->
        parent.upload pth.basename(path), path , uploadCallback(path,cb)
        return
      q.push fn
      q.start()
      return
  
    upFile = uploadTree.get path

    unless upFile #make uploaded file is still in the uploadTree
      cb()
      return
    uploadedFileLocation = pth.join uploadLocation, upFile.cache

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
      inodes = value.inode for value in folderTree.values()
      inode = Math.max(inodes) + 1
      file = new GFile(result.downloadUrl, result.id, result.parents[0].id, result.title, parseInt(result.fileSize), (new Date(result.createdDate)).getTime(), (new Date(result.modifiedDate)).getTime(), inode, true)        
      inodeToPath.set inode, path
    client.idToPath.set( result.id, path)
    #update folder Tree
    if  file.name not in parent.children
      parent.children.push file.name
    folderTree.set path, file
    client.saveFolderTree()

    #move the file to download folder after finished uploading
    fs.open uploadedFileLocation, 'r', (err,fd) ->
      if err
        logger.debug "could not open #{uploadedFileLocation} for copying file from upload to uploader"
        logger.debug err
        return

      moveToDownload(file, fd, uploadedFileLocation, 0, cb)
      return

    return

#resume file uploading
resumeUpload = ->  
  # uploadWork = null
  if uploadTree.count() > 0
    logger.info "resuming file uploading"
    paths = uploadTree.keys()
    uploadTree.forEach (value,path) ->
      parentPath = pth.dirname(path)
      if folderTree.has parentPath
        parent = folderTree.get parentPath
        if parent instanceof GFolder
          q.push (cb) ->
            parent.upload pth.basename(path), path, uploadCallback(path,cb)
            return
          q.start()
        else
          logger.debug "While resuming uploads, #{parentPath} was not a folder"
      return
  return

start = ->
  if folderTree.count() > 1
    try
      logger.log "info", 'attempting to start f4js'
      switch os.type()
        when 'Linux' 
          add_opts = []
          command = "fusermount -u #{config.mountPoint}"          
        when 'Darwin'
          add_opts = ["-o",'daemon_timeout=0', "-o", "noappledouble", "-o", "noubc"]
          command = "diskutil umount force #{config.mountPoint}"
        else 
          add_opts = []
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
        opts =  ["GDrive", "-o", "allow_other", config.mountPoint]
        # opts.push "-s"
        # opts.push "-f"

        # opts.push "-mt"
        # opts.push "-d"
        fuse.fuse.mount
          filesystem: GDriveFS
          options: opts.concat(add_opts)
        logger.log('info', "mount point: #{config.mountPoint}")
        setTimeout resumeUpload, 8000
        return
    catch e
      logger.log( "error", "Exception when starting file system: #{e}")
  else
    setTimeout start, 500
  return
  
start()  
