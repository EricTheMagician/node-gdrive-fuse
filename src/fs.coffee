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
inodeTree = client.inodeTree
idToInode = client.idToInode
drive = client.drive
folder = require("./folder")
uploadTree = folder.uploadTree
GFolder = folder.GFolder
saveUploadTree = folder.saveUploadTree
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
    # logger.silly "getting attr for #{path}"
    if inodeTree.has(inode)
      callback = (status, attr)->
        reply.attr(attr, 5)
        return
      inodeTree.get(inode).getAttr(callback)
        
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
    # console.log path
    if inodeTree.has(inode)
      object = inodeTree.get(inode)
      if object instanceof GFile
        reply.err errnoMap.ENOTDIR
      else if object instanceof GFolder
        size = Math.max( requestedSize , object.children.length * 256)
        # size = requestedSize
        parent = inodeTree.get object.parentid
        totalSize = 0
        #totalSize += reply.addDirEntry('.', requestedSize, {inode: object.inode}, offset);
        # totalSize += reply.addDirEntry('..', requestedSize, {inode: parent.inode}, offset);
        for child in object.children
          cnode = inodeTree.get child
          if cnode
            attr = cnode.getAttrSync()
            len = reply.addDirEntry(cnode.name, size, {inode: cnode.inode}, offset);          
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
    logger.debug "setting attr for #{inode}"
    console.log attrs
    file = inodeTree.get inode
    unless file
      reply.err errnoMap.ENOENT
      return
    # console.log file
    # console.log attrs
    a = new Date(attrs.atime)
    m = new Date(attrs.mtime)
    # console.log a.getTime(),m.getTime()
    # attrs.atime = a.getTime()
    # attrs.mtime = m.getTime()
    file.mtime = m.getTime()
    if attrs.hasOwnProperty("size")
      file.size = attrs.size

    if attrs.hasOwnProperty("mode")
      logger.debug "mode before and after: #{file.mode}-#{attrs.mode}"
      file.mode = attrs.mode

    inodeTree.set inode, file


    reply.attr(file.getAttrSync(), 5);
    # reply.err(0)
    return

  open: (context, inode, fileInfo, reply) ->
    flags = fileInfo.flags
    if flags.rdonly #read only
      if inodeTree.has(inode)
        file = inodeTree.get(inode)
        if file instanceof GFile
          if file.downloadUrl #make sure that the file has been fully uploaded
            reply.open(fileInfo)
          else
            reply.err errnoMap.EACCESS
        else
          reply.errerrnoMap.EISDIR
      else
        reply.err errnoMap.ENOENT
      return

    if flags.wronly #write only
      logger.silly "tried to open file \"#{inode}\" for writing"
      if inodeTree.has(inode) #if folderTree has path, make sure it's a file with size zero
        file = inodeTree.get(inode)
        if (file instanceof GFile)
         if file.size == 0
          # logger.debug "#{path} size was 0"
          if uploadTree.has(inode)
            cache = uploadTree.get(inode).cache
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
    # logger.log "silly", "reading file #{path} - #{offset}:#{len}"

    if inodeTree.has(inode)
      once = false
      callback = (dataBuf) ->
        unless once
          once = true
          reply.buffer(dataBuf, dataBuf.length)
        return

      #make sure that we are only reading a file
      file = inodeTree.get(inode)
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

    # path = inodeToPath.get inode
    # logger.log "silly", "writing to file #{path} - position: #{position}, length: #{buffer.length}"

    file = inodeTree.get inode  
    unless file
      logger.debug inode
      reply.err errnoMap.ENOENT
      return
    size = file.size
    fs.write fileInfo.fh, buffer, 0, buffer.length, position, (err, bytesWritten, buffer) ->
      if (err)
        logger.debug "there was an error writing for file #{file.name}"
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
    # parentPath = inodeToPath.get parentInode
    # path = pth.join parentPath, name
    # logger.debug "creating folder #{path}"
    logger.debug "creating folder #{name}"
    parent = inodeTree.get parentInode
    if parent #make sure that the parent exists
      if parent instanceof GFolder #make sure that the parent is a folder

        for childInode in parent.children #make sure that the child doesn't already exist
          child = inodeTree.get childInode
          if child.name == name
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
            inodes = value.inode for value in inodeTree.values()
            inode = Math.max(inodes) + 1
            parent.children.push inode
            folder = new GFolder(res.id, res.parents[0].id, name, (new Date(res.createdDate)).getTime(), (new Date(res.modifiedDate)).getTime(), inode, res.editable, [])            
            inodeTree.set inode, folder
            idToInode.set folder.id, inode
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
    parent = inodeTree.get parentInode
    logger.log "debug", "removing folder #{name}"

    #make sure the actual directory exists
    for childInode in parent.children
      folder = inodeTree.get childInode
      if folder.name == name  

        # make sure that it is a folder
        if folder instanceof GFolder  
          #make sure it is empty
          if folder.children.length == 0
            drive.files.trash {fileId: folder.id}, (err, res) ->
              if err
                logger.log "error", "unable to remove folder #{path}"
                reply.err errnoMap.EIO
                return
              else
                idx = parent.children.indexOf childInode
                if idx >= 0
                  parent.children.splice idx, 1
                inodeTree.remove childInode
                idToInode.remove(folder.id)

                reply.err 0
                client.saveFolderTree()
                return
              return  
            
            return
          else
            reply.err errnoMap.ENOTEMPTY
            return
        else
          reply.err errnoMap.ENOTDIR
          return

    reply.err errnoMap.ENOENT
    return

  mknod: (context, parentInode, name, mode, rdev, reply) ->
      

    parent = inodeTree.get parentInode

    for childInode in parent.children #TODO: if file exists, delete it first
      child = inodeTree.get(childInode)
      if child and child.name == name
        reply.err PosixError.EEXIST
        return

    now = (new Date).getTime()
    inodes = value.inode for value in inodeTree.values()
    inode = Math.max(inodes) + 1

    file = new GFile(null, null, parent.id, name, 0, now, now, inode, true)
    inodeTree.set inode, file
    parent.children.push inode

    logger.debug "mknod: parentid: #{parent.id} -- inode #{inode}"
    logger.info "adding a new file #{name} to folder #{parent.name}"
    attr = file.getAttrSync()

    upFile = 
      cache: MD5(parent.id + name)
      uploading: false
    uploadTree.set inode, upFile
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
    parent = inodeTree.get parentInode
    
    if parent #make sure parent exists
      logger.log "debug", "creating file #{name}"

      cache = MD5(parent.id + name)
      systemPath = pth.join(uploadLocation, cache);

      # for childInode in parent.children #TODO: if file exists, delete it first
      #   parent.children.push name
      now = (new Date).getTime()
      logger.log "debug", "adding file \"#{name}\" to folder \"#{parent.name}\""
      inodes = value.inode for value in inodeTree.values()
      inode = Math.max(inodes) + 1
      file = new GFile(null, null, parent.id, name, 0, now, now, inode, true)
      inodeTree.set inode, file
      parent.children.push inode

      logger.debug "create: parentid: #{parent.id} -- inode #{inode}"
      logger.info "adding a new file #{name} to folder #{parent.name}"


      client.saveFolderTree()


      fs.open systemPath, 'w', (err, fd) ->
        if (err)
          logger.log "error", "unable to create file #{inode} -- #{name}, #{err}"
          reply.err(errnoMap[err.code])
          return
        fileInfo.fh = fd
        logger.log "debug", "setting upload Tree"
        upFile = 
          cache: cache
          uploading: false
        uploadTree.set inode, upFile
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
    logger.log "debug", "removing file #{name}"
    parent = inodeTree.get parentInode

    for childInode in parent.children
      file = inodeTree.get childInode

      #make sure the file still exists in the inodeTree
      #if not, remove it
      unless file
        idx = parent.children.indexOf childInode
        parent.children.splice(idx,1)
        continue

      #make sure it's the right file
      if file.name != name
        continue

      if file instanceof GFolder
        reply.err errnoMap.EISDIR    
        return


      parent.children.splice( parent.children.indexOf(childInode), 1)
      inodeTree.remove childInode
      idToInode.remove file.id
      client.saveFolderTree()

      drive.files.trash {fileId: file.id}, (err, res) ->
        if err
          logger.log "debug", "unable to remove file #{file.name}"
        reply.err 0 #always return success
        return          

      if uploadTree.has childInode
        cache = uploadTree.get(childInode).cache
        uploadTree.remove childInode
        fs.unlink pth.join(uploadLocation,cache), (err) ->
          return

      return


    reply.err PosixError.ENOENT
    return
    

  # /*
  #  * Handler for the release() system call.
  #  * path: the path to the file
  #  * fd:  the optional file handle originally returned by open(), or 0 if it wasn't
  #  * cb: a callback of the form cb(err), where err is the Posix return code.
  #  */
  release: (context, inode, fileInfo, reply) ->
    logger.silly "closing file #{inode}"
    if uploadTree.has inode
      logger.log "debug", "#{inode} was in the upload tree"
      #close the file
      fs.close fileInfo.fh, (err) ->
        if (err)
          reply.err err.errno
          return
        reply.err(0)

        #upload file once file is closed
        if uploadTree.has inode
          upCache = uploadTree.get inode
          upCache.released = true
          uploadTree.set inode, upCache
          saveUploadTree()

          file = inodeTree.get(inode)
          parentInode = idToInode.get file.parentid
          parent = inodeTree.get parentInode
          ###
          three cases: 
          if file size is 0: delete it and don't upload
          if file size is <=10MB, just upload it directly
          if file size is >10 MB, add to upload queue
          ###


          if 0 < file.size <=  10485760 #10MB 
            cb = ->
              return
            parent.upload file.name, inode, uploadCallback(inode, cb)           
          else if file.size >  10485760           
            fn = (cb)->
              if parent instanceof GFile
                logger.debug "While uploading, #{name} was a file - #{parent}"
                cb()
                return
              parent.upload file.name, inode, uploadCallback(inode,cb)            
              return
            q.push fn
            q.start()
          else          
            uploadTree.remove inode
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

  getxattr: (context, parentInode, name, size, position, reply) ->
    console.log('GetXAttr was called!')
    parent = inodeToPath.get parentInode
    for childInode in parent.children
      if inodeTree.get(childInode).name == name
        reply.err 0
        return
    reply.err PosixError.ENOENT
    return
  listxattr: (context, inode, size, reply) ->
    console.log "listxattr called"
    obj = inodeTree.get inode
    if obj
      console.log obj

    reply.xattr 1024*1024
  access: (context, inode, mask, reply) ->
    console.log('Access was called!');
    reply.err(0);
    return

  rename: (context, oldParentInode, oldName, newParentInode, newName, reply) ->    
    #find the currrent child
    parent = inodeTree.get oldParentInode
    unless parent
      reply.err PosixError.ENOENT
      return

    for childInode in parent.children
      child = inodeTree.get childInode
      if child.name == oldName
        #move to new folder if required
        params = 
          resource:
            title: newName
          fileId: child.id
          modifiedDate: true
        if newParentInode != oldParentInode
          newParent = inodeTree.get newParentInode
          unless newParent
            reply.err PosixError.ENOENT
            return
          unless newParent instanceof GFolder
            reply.err PosixError.ENOTDIR
            return
          params.addParents = newParentInode.id
          params.removeParents =  parent.id

        child.name = newName
        console.log "before google api"
        drive.files.patch params, (err)->
          console.log "after google api"
          if err
            logger.error "There was an error with renaming file #{child.name}"
            logger.error err
            reply.err PosixError.EIO
            return
          reply.err 0
          if newParentInode != oldParentInode
            newParent.children.push childInode
            oldParent.children.splice( oldParent.children.indexOf(childInode), 1 )
          return



        return

    #if we get here, it means there was no child found
    reply.err PosixError.ENOENT
    return
  lookup: (context, parentInode, name, reply) ->

      #make sure the parent inode exists
      unless inodeTree.has parentInode
        reply.err PosixError.ENOENT

      parent = inodeTree.get parentInode
      for childInode in parent.children      
        child = inodeTree.get(childInode)
        if child and child.name == name
          attr = child.getAttrSync()
          attr.size ||= 4096
          entry = 
              inode: childInode
              generation: 2
              attr: attr
              # attr_timeout: 5,
              # entry_timeout: 5
          reply.entry(entry)
          return

      #if the child is not found
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
uploadCallback = (inode, cb) ->
  file = inodeTree.get inode
  parentInode = idToInode.get file.parentid
  parent = inodeTree.get parentInode

  return (err, result) ->
    if err
      if err == "invalid mime"
        logger.debug "the mimetype of #{path} was invalid"
        cb()
        return
      if err == "uploading"
        cb()
        return
      if err.code == "ENOENT"
        uploadTree.remove(inode)
        cb()
        return

      cb()
      logger.debug "Retrying upload: \"#{file.name}\"."
      fn = (_cb) ->
        parent.upload file.name, inode , uploadCallback(inode,_cb)
        return
      q.push fn
      q.start()
      return
  
    upFile = uploadTree.get inode

    unless upFile #make sure uploaded file is still in the uploadTree
      cb()
      return
    uploadedFileLocation = pth.join uploadLocation, upFile.cache

    logger.log 'info', "successfully uploaded #{file.name}"
        
    uploadTree.remove inode
    saveUploadTree()
    if inodeTree.has inode
      logger.debug "#{file.name} already existed in inodeTree"
      file = inodeTree.get inode
      file.downloadUrl = result.downloadUrl
      file.id = result.id
      file.size = parseInt(result.fileSize)
      file.ctime = (new Date(result.createdDate)).getTime()
      file.mtime =  (new Date(result.modifiedDate)).getTime()
    else
      logger.debug "#{file.name} folderTree did not exist"     
      inodes = value.inode for value in folderTree.values()
      inode = Math.max(inodes) + 1
      file = new GFile(result.downloadUrl, result.id, result.parents[0].id, result.title, parseInt(result.fileSize), (new Date(result.createdDate)).getTime(), (new Date(result.modifiedDate)).getTime(), inode, true)        

    #update parent
    if  file.inode not in parent.children
      parent.children.push file.inode
    inodeTree.set inode, file
    idToInode.set file.id, inode
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
    inodes = uploadTree.keys()
    uploadTree.forEach (value,inode) ->
      if inodeTree.has(inode)
        file = inodeTree.get(inode)
      else
        uploadTree.remove inode
        return

      #check to see if the file was released by the filesystem
      #if it wasn't released by the filesystem, it means that the file was not finished transfering
      if value.released
        parentInode = idToInode.get( file.parentid )
        value.uploading = false
        if inodeTree.has parentInode
          parent = inodeTree.get parentInode
          if parent instanceof GFolder
            inodeTree.set key, value
            q.push (cb) ->
              parent.upload file.name, inode, uploadCallback(inode,cb)
              return
            q.start()
          else
            logger.debug "While resuming uploads, #{parent} was not a folder"
        return
      else
        inodeTree.remove inode
        uploadTree.remove inode
        parentInode = idToInode.get value.parentid
        parent = inodeTree.get parentInode
        if parent
          idx = parent.children.indexOf inode
          if idx > 0
            parent.children.splice idx, 1
        path = pth.join uploadLocation, value.cache
        fs.unlink path, ->
          return

  return


start = ->
  if inodeTree.count() > 1
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

      debug = false

      exec command, (err, data) ->
        fs.ensureDirSync(config.mountPoint)
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
