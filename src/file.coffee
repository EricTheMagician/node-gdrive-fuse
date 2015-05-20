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

    ws = null
    once = false
    request(options)
    .on 'response', (resp) ->
      if resp.statusCode == 401 or resp.statusCode == 403
        unless once
          once = true
          fn = ->
            cb("expiredUrl")
            return
          setTimeout fn, 2000
      if resp.statusCode >= 500
        unless once
          fn = ->
            cb(500)
      return
    .on 'error', (err)->
      unless once
        once = true
        console.log "error"
        console.log err
        console.log err.code
        if err.code == "EMFILE"
          logger.debug "There was an error with downloading files: EMFILE"
          logger.debug err
          openedFiles.forEach (value, key) ->
            clearTimeout(value.to)
            fs.close value.fd, ->
              return
            return


        cb(err)
      return
    .pipe(
      fs.createWriteStream(saveLocation)
    ).on 'close', ->
      unless once
        once = true
        cb(null)
      return
    

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
      if openedFiles.has("#{file.id}-#{start}")
        fs.close openedFiles.get("#{file.id}-#{start}").fd, (err) ->
          openedFiles.remove "#{file.id}-#{start}"
          return
      return   
    cacheTimeout = 3000    
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
            logger.silly "there was an debug stat-ing a file in file.open"
            logger.silly err
            cb err,false
            return
          if stats.size == (end - start + 1)
            fd = fs.open path, 'r', (err,fd) ->
              if err
                if err.code == "EMFILE"
                  for o in openedFiles.values()
                    clearTimeout o.to
                    fs.close o.fd, ->
                      return
                  file.open(start, cb)
                else
                  logger.error "there was an handled error while opening files for reading"
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
        catch error
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

module.exports.GFile = GFile
