pth = require 'path'
fs = require 'fs-extra'
hashmap = require( 'hashmap' ).HashMap
winston = require 'winston'
{EventEmitter} = require 'events'
request = require 'request'
######################################
######### Setup File Config ##########
######################################
if fs.existsSync 'config.json'
  config = fs.readJSONSync 'config.json'
else
  config = {}

config.cacheLocation ||=  '/tmp/cache'

#download location
downloadLocation = pth.join config.cacheLocation, 'download'
fs.ensureDirSync downloadLocation

#upload location
uploadLocation = pth.join config.cacheLocation, 'upload'
fs.ensureDirSync uploadLocation

printDate = ->
  d = new Date
  return "#{d.getFullYear()}-#{d.getMonth()+1}-#{d.getDate()}T#{d.getHours()}:#{d.getMinutes()}::#{d.getSeconds()}"

#setup winston logger
transports = [new (winston.transports.File)({ 
  filename: '/tmp/GDriveF4JS.log', 
  level:'debug' ,
  maxsize: 10485760, #10mb
  maxFiles: 3
  })]
if config.debug
  transports.push new (winston.transports.Console)({ level: 'debug', timestamp: printDate,colorize: true })
else
  transports.push new (winston.transports.Console)({ level: 'info', timestamp: printDate,colorize: true })

logger = new (winston.Logger)({
    transports: transports
})

module.exports.logger = logger
config.advancedChunks ||= 5

#setup cache size monitoring
sqlite3 = require 'sqlite3'
queue = require 'queue'
db = new sqlite3.Database(pth.join(config.cacheLocation, 'data','sqlite.db'));
q = queue({concurrency: 1, timeout: 7200000 })
totalDownloadSize = 0
regexPattern = ///^[a-zA-Z0-9-]*-([0-9]*)-([0-9]*)$///
if config.maxCacheSize
  maxCache =  config.maxCacheSize * 1024 * 1024
else
  logger.info "max cache size was not set. you should exit and manually set it"
  logger.info "defaulting to a 10 GB cache"
  maxCache  = 10737418240


#opened files
openedFiles = new hashmap()
downloadTree = new hashmap()
buf0 = new Buffer(0)



######################################
######### Create File Class ##########
######################################


class GFile extends EventEmitter

  @chunkSize: 1024*1024*16 #set default chunk size to 16. this should be changed at run time

  constructor: (@downloadUrl, @id, @parentid, @name, @size, @ctime, @mtime, @inode, @permission, @mode = 0o100777) ->

  @download: (url, start,end, size, saveLocation, cb ) -> 
    if config.accessToken == null
      logger.debug "access token was null when downloading files"
      cb("expiredUrl")
      return

    options =
      url: url
      encoding: null
      headers:
        "Authorization": "Bearer #{config.accessToken.access_token}"
        "Range": "bytes=#{start}-#{end}"

    ws = fs.createWriteStream(saveLocation) 
    ws.on 'error', (err) ->        
      logger.error "There was an error with writing during the download"
      logger.error err
      if err.code == "EMFILE"
        logger.debug "There was an error with downloading files: EMFILE"
        logger.debug err
      cb(err)
      this.end()
      return        
    once = false
    try    
      request(options)
      .on 'response', (resp) ->
        if resp.statusCode == 401 or resp.statusCode == 403
          unless once
            once = true
            fn = ->
              cb("expiredUrl")
              return
            setTimeout fn, 2000
          ws.end()
        if resp.statusCode >= 500
          unless once
            fn = ->
              cb(500)
          ws.end()
        return
      .on 'error', (err)->
        unless once
          once = true
          logger.error "error"
          logger.error err
          logger.error err.code
          if err.code == "EMFILE"
            logger.debug "There was an error with downloading files: EMFILE"
            logger.debug err

          cb(err)
        this.end()     
        ws.end()     
        return
      .pipe( 
        ws
      )
      .on 'error', (err) ->        
        logger.error "There was an error with piping during the download"
        logger.error err
        if err.code == "EMFILE"
          logger.debug "There was an error with downloading files: EMFILE"
          logger.debug err
        cb(err)
        this.end()
        ws.end()
        return        
      .on 'close', ->
        unless once
          once = true
          ws.end()
          base = pth.basename(saveLocation)
          chunkSize = end-start + 1
          addNewFile(base,'downloading', chunkSize )
          cb(null)
        return
      return
    catch e
      logger.error "There was an uncaught error while downloading"
      logger.error e
      ws.end()
    

    return
  
  getAttrSync: () =>
    attr =
      mode: @mode,
      size: @size,
      nlink: 1,
      mtime: @mtime,
      ctime: @ctime
      inode: @inode
    return attr

  getAttr: (cb) =>
    attr =
      mode: @mode,
      size: @size,
      nlink: 1,
      mtime: @mtime,
      ctime: @ctime,
      inode: @inode
    cb(0,attr)
    return

  recursive: (start,end) =>
    file = @
    path = pth.join(downloadLocation, "#{file.id}-#{start}-#{end}")
    if start >= @size
      return
    file.open start, (err, fd) ->
      if err or fd == false
        unless downloadTree.has("#{file.id}-#{start}")
          logger.silly "starting to recurse #{file.name}-#{start}"
          downloadTree.set("#{file.id}-#{start}", 1)
          callback =  (err) ->
            # if err
              #logger.debug "There was an error during recursive download #{start}-#{end}:"
              #logger.debug err
            downloadTree.remove("#{file.id}-#{start}")
            file.emit 'downloaded', start
            fn = ->
              file.emit 'downloaded', start
            setTimeout fn, 1000
            logger.silly "finishing recurse #{file.id}-#{start}"
            return

          GFile.download(file.downloadUrl, start,end, file.size, path, callback)
          # file.download(start, end,false, callback)
          return

    return

  open: (start,cb) =>
    file = @
    fn = ->
      opened = openedFiles.has("#{file.id}-#{start}")
      if opened 
        unless opened.fd
          logger.debug "opened.fd was false"
          logger.debug file
          logger.debug opened
          return 
        fs.close openedFiles.get("#{file.id}-#{start}").fd, (err) ->
          if err
            logger.error "There was an error with closing file #{file.name}"
            logger.error err
          openedFiles.remove "#{file.id}-#{start}"
          return
      return   
    cacheTimeout = 6000    
    if openedFiles.has( "#{file.id}-#{start}")
      f = openedFiles.get "#{file.id}-#{start}"
      clearTimeout(f.to)
      f.to = setTimeout(fn, cacheTimeout)
      cb null, f.fd
      return

    else
      end = Math.min(start + GFile.chunkSize, file.size ) - 1
      path = pth.join(downloadLocation, "#{file.id}-#{start}-#{end}")
      try
        fs.stat path, (err, stats) ->
          if err
            logger.silly "there was an error stat-ing a file in file.open"
            logger.silly err
            cb err,false
            return
          if stats.size == (end - start + 1)
            fs.open path, 'r', (err,fd) ->
              if err
                if err.code == "EMFILE"
                  file.open(start, cb)
                else
                  logger.error "there was an handled error while opening files for reading"
                  logger.error err
                  cb(err)
                return
              
              #make sure that there's only one file opened.
              #multiple files can be opened at once because of the fuse multithread
              if openedFiles.has "#{file.id}-#{start}"
                opened = openedFiles.get("#{file.id}-#{start}")
                clearTimeout opened.to

                cb null, opened.fd
                fs.close fd, (err) ->
                  if err
                    logger.error "There was an error closing an already opened file"
                    logger.error err
                  return

                opened.to = setTimeout(fn, cacheTimeout)
                return

              openedFiles.set "#{file.id}-#{start}", {fd: fd, to: setTimeout(fn, cacheTimeout) }
              cb null, fd
              return
          else
            cb null, false
          return
      catch
        cb null, false
      return
  read: (start,end, readAhead, cb) =>
    file = @
    end = Math.min(end, @size-1)
    chunkStart = Math.floor((start)/GFile.chunkSize)* GFile.chunkSize
    chunkEnd = Math.min( Math.ceil(end/GFile.chunkSize) * GFile.chunkSize, file.size)-1
    nChunks = (chunkEnd - chunkStart)/GFile.chunkSize

    if nChunks < 1
      _readAheadFn = ->
        if readAhead
          if chunkStart <= start < (chunkStart + 131072)
            file.recursive( Math.floor(file.size / GFile.chunkSize) * GFile.chunkSize, file.size-1)
            file.recursive(chunkStart + i * GFile.chunkSize, chunkEnd + i * GFile.chunkSize) for i in [1..config.advancedChunks]
        return


      path = pth.join(downloadLocation, "#{file.id}-#{chunkStart}-#{chunkEnd}")
      if downloadTree.has("#{file.id}-#{chunkStart}")
        logger.silly "download tree has #{file.id}-#{chunkStart}"
        __once__ = false
        listenCallback = (cStart)  ->
          unless __once__
           #logger.silly "listen callback #{file.id}-#{chunkStart},#{cStart}"
           if ( cStart <= start < (cStart + GFile.chunkSize-1)  )
              #logger.debug "once #{ __once__ } -- #{cStart} -- #{start}"
              __once__ = true
              file.removeListener 'downloaded', listenCallback
              #logger.silly "listen callback #{file.id}-#{chunkStart}"

              # we need to re-emit because of the -mt flag from fuse.
              # otherwise, this 
              file.emit 'downloaded', cStart
              file.read(start,end, readAhead, cb)

          return

        file.on 'downloaded', listenCallback
        _readAheadFn()
        return

      #try to open the file or get the file descriptor
      file.open chunkStart, (err,fd) ->

        #fd can returns false if the file does not exist yet
        if err or fd == false
          file.download start, end, readAhead, cb
          _readAheadFn()
          return

        downloadTree.remove("#{file.id}-#{chunkStart}")

        #if the file is opened, read from it
        readSize = end-start;
        buffer = new Buffer(readSize+1)
        try
          fs.read fd,buffer, 0, readSize+1, start-chunkStart, (err, bytesRead, buffer) ->
            cb(buffer.slice(0,bytesRead))
            return
          _readAheadFn()
        catch e
          logger.error "There was an error while reading file. Retrying"
          logger.error e
          file.read(start,end, readAhead, cb)

        return


    else if nChunks < 2
      end1 = chunkStart + GFile.chunkSize - 1
      start2 = chunkStart + GFile.chunkSize

      callback1 = (buffer1) ->
        if buffer1.length == 0
          cb(buffer1)
          return
        callback2 = (buffer2) ->
          if buffer2.length == 0
            cb(buffer1)
            return
          cb( Buffer.concat([buffer1, buffer2]) )
          return

        file.read( start2, end, true, callback2)
        return

      file.read( start, end1,true, callback1)

    else
      logger.debug "too many chunks requested, #{nChunks}"
      cb(buf0)

    return

  updateUrl: (cb) =>
    logger.debug "updating url for #{@name}"
    file = @
    data = 
      fileId: @id
      acknowledgeAbuse  : true
      fields: "downloadUrl"    
    GFile.GDrive.files.get data, (err, res) ->
      if err
        logger.error "There was an error while getting an updated url for #{file.name}"
        logger.error err
        file.updateUrl(cb)
        return
      file.downloadUrl = res.downloadUrl
      
      GFile.oauth.refreshAccessToken (err, tokens) ->

        config.accessToken = tokens
        if err
          logger.silly "there was an error while updating url"
          logger.silly "err", err
        cb(file.downloadUrl)
        return
      return
    return

  download:  (start, end, readAhead, cb) =>
    #if file chunk already exists, just download it
    #else download it    
    file = @
    chunkStart = Math.floor((start)/GFile.chunkSize) * GFile.chunkSize
    chunkEnd = Math.min( Math.ceil(end/GFile.chunkSize) * GFile.chunkSize, file.size)-1 #and make sure that it's not bigger than the actual file
    nChunks = (chunkEnd - chunkStart)/GFile.chunkSize

    if nChunks < 1
      path = pth.join(downloadLocation, "#{file.id}-#{chunkStart}-#{chunkEnd}")
      callback = (err)->   
        if err
          if err == "expiredUrl"
            fn = (url) ->
              GFile.download(url, chunkStart, chunkEnd, file.size, path, callback)
              return
            file.updateUrl(fn)       
          else
            logger.error "there was an error downloading file"
            logger.error err
            cb(buf0)
            downloadTree.remove("#{file.id}-#{chunkStart}")
            file.emit 'downloaded', chunkStart
            fn = ->
              file.emit 'downloaded', chunkStart
            setTimeout fn, 1000
          return

        downloadTree.remove("#{file.id}-#{chunkStart}")
        file.read(start,end, readAhead, cb)
        file.emit 'downloaded', chunkStart
        return
      if downloadTree.has "#{file.id}-#{chunkStart}" 
        file.read(start,end,readAhead,cb)
      else
        logger.debug "starting to download #{file.name}, chunkStart: #{chunkStart}"      
        downloadTree.set("#{file.id}-#{chunkStart}", 1)
        GFile.download(file.downloadUrl, chunkStart, chunkEnd, file.size, path, callback)

    else if nChunks < 2      
      end1 = chunkStart + GFile.chunkSize - 1
      start2 = chunkStart + GFile.chunkSize

      callback1 = (buffer1) ->
        if buffer1.length == 0
          cb(buffer1)
          return
        callback2 = (buffer2) ->
          if buffer2.length == 0
            cb(buffer1)
            return
          cb( Buffer.concat([buffer1, buffer2]) )
          return

        file.read( start2, end, true, callback2)
        return

      file.read( start, end1,true, callback1)

    else
      logger.debug "too many chunks requested, #{nChunks}"
      cb(buf0)

    return

#### prevent download folder from getting too big
queue_fn = (size, cmd) ->
  fn = (done) ->
    db.run cmd, (err) ->
      if err
        logger.error "init run path - #{cmd}"
        logger.error err
        done()
        return

      totalDownloadSize += size
      if totalDownloadSize > 0.90*maxCache
        delete_files()
      logger.silly "totalDownloadSize: #{totalDownloadSize}"
      done()
      return
  return fn
initialize_path = (path, type) ->
  fs.readdir path, (err, files) ->
    count = 0
    totalSize = 0
    basecmd = "INSERT OR REPLACE INTO files (name, atime, type, size) VALUES "
    cmd = basecmd
    for file in files
      expectedSize = file.match(regexPattern)
      if(expectedSize != null)
        size = Math.max(parseInt(expectedSize[2])- parseInt(expectedSize[1]) + 1, 0)
        if size == 0
          logger.debug "expectedSize for #{file} is 0. #{expectedSize}"
        cmd += "('#{file}', 0, '#{type}', #{size})"        
        count += 1
        totalSize += size

        if count > 750
          q.push queue_fn(totalSize, cmd)
          count = 0
          totalSize = 0
          cmd = basecmd
        else
          cmd += ','          
      else
        logger.debug "expectedSize is null for this file: #{file}"



    #Make sure the queue is empty
    if count > 0
      q.push queue_fn(totalSize, cmd.slice(0,-1)) #remove the last comma
      count = 0
      totalSize = 0
      cmd = basecmd

    q.start()
    return
  return
delete_once = false
delete_files = ->  
  unless delete_once
    delete_once = true
    logger.info "deleting files to make space in the cache"
    logger.info "current size of cache is: #{totalDownloadSize/1024/1024} GB"

    db.all "SELECT * from files ORDER BY atime, size ASC", (err, rows) ->
      _delete_files_(0,0,rows)
      return
    return
  return

_delete_files_ = (start,end, rows) ->
  row = rows[end]
  count = end - start + 1
  if totalDownloadSize >= (0.8*maxCache)
    fs.unlink pth.join(downloadLocation, row.name), (err) ->
      unless err
        #if there is an error, it usually is because there was a file that was in the db that was already deleted
        totalDownloadSize -= row.size

      if count > 200
        cmd = "DELETE FROM files WHERE name in ("
        for row in rows[start...end]         
          cmd += "'#{row.name}',"
        cmd += "'#{rows[end].name}')"
      
        db.run cmd, (err) ->
          if err
            logger.error "There was an error with database while deleting files"
            logger.err err
            delete_once = false
            logger.info "finsihed deleting files by error"
            logger.info "current size of cache is: #{totalDownloadSize/1024/1024} GB"
            return

          end += 1
          if end == rows.length
            logger.info "finsihed deleting files by delelting all files"
            logger.info "current size of cache is: #{totalDownloadSize/1024/1024} GB"
            delete_once = false
          else
            _delete_files_(end, end, rows)
          return


      else 
        end += 1
        if end == rows.length
          logger.info "finsihed deleting files by delelting all files"
          logger.debug "and then running the database cmd"
          logger.info "current size of cache is: #{totalDownloadSize/1024/1024} GB"
          delete_once = false
        else
          _delete_files_(end, end, rows)
      return
  else
    if end > start
      cmd = "DELETE FROM files WHERE name in ("
      for row in rows[start...end]         
        cmd += "'#{row.name}',"
      cmd += "'#{rows[end].name}')"
      
      db.run cmd, (err) ->
        if err
          logger.error "There was an error with database while final deleting files"
          logger.error err
        return
    logger.info "finished deleting files"
    logger.info "current size of cache is: #{totalDownloadSize/1024/1024} GB"
    delete_once = false


  return





addNewFile = (file, type, size)->
  # db.run "INSERT OR REPLACE INTO files (name, atime, type, size) VALUES ('#{file}', #{Date.now()}, '#{type}', #{size})", ->
  #   totalDownloadSize += size
  #   console.log totalDownloadSize
  cmd = "INSERT OR REPLACE INTO files (name, atime, type, size) VALUES ('#{file}', #{Date.now()}, '#{type}', #{size})"
  q.push queue_fn(size,cmd)
  q.start()
  return

db.run  "CREATE TABLE IF NOT EXISTS files (size INT, name TEXT unique, type INT, atime INT)", (err) ->
  if err
    logger.log err
  logger.info "Opened a connection to the database"
  # initialize_db()
  initialize_path downloadLocation, "downloading"

module.exports.GFile = GFile
module.exports.addNewFile = addNewFile
module.exports.queue_fn = queue_fn