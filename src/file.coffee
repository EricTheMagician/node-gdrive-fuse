pth = require 'path'
fs = require 'fs-extra'
hashmap = require( 'hashmap' ).HashMap
Fiber = require 'fibers'
Future = require 'fibers/future'
rest = require 'restler'
winston = require 'winston'

logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'info' }),
      new (winston.transports.File)({ filename: '/tmp/GDriveF4JS.log', level:'debug' })
    ]
  })

######################################
######### Setup File Config ##########
######################################

config = fs.readJSONSync 'config.json'

#download location
downloadLocation = pth.join config.cacheLocation, 'download'
fs.ensureDirSync downloadLocation

#upload location
uploadLocation = pth.join config.cacheLocation, 'upload'
fs.ensureDirSync uploadLocation

downloadTree = new hashmap()


google = require 'googleapis'
oauth2Client = new google.auth.OAuth2(config.clientId, config.clientSecret, config.redirectUrl)
refreshToken =  () ->
  oauth2Client.refreshAccessToken (err,tokens) ->
    config.accessToken = tokens
    fs.outputJsonSync 'config.json', config
oauth2Client.setCredentials config.accessToken


######################################
######### Wrap fs functions ##########
######################################

writeFile = Future.wrap(fs.writeFile)
open = Future.wrap(fs.open)
read = Future.wrap(fs.read,5)
stats = Future.wrap(fs.stat)
writeFile = Future.wrap(fs.writeFile)

#since fs.exists does not return an error, wrap it using an error
exists = Future.wrap (path, cb) ->
  fs.exists path, (success)->
    cb(null,success)

close = Future.wrap (path,cb) ->
  fs.close path, (err) ->
    cb(err, true)


######################################
######### Create File Class ##########
######################################

class GFile

  @chunkSize: 1024*1024*16 #set default chunk size to 16. this should be changed at run time

  constructor: (@downloadUrl, @id, @parentid, @name, @size, @ctime, @mtime, @permission) ->

  @download = (url, start,end, size, cb ) ->
    rest.get url, {
      decoding: "buffer"
      timeout: 300000
      headers:
        "Authorization": "Bearer #{config.accessToken.access_token}"
        "Range": "bytes=#{start}-#{end}"
    }
    .on 'complete', (result, response) ->
      if result instanceof Error
        cb(result)
      else
        #check to see if token is expired
        if response.statusCode == 401
          refreshToken()
          GFile.download(url, start,end, size,cb )
        else
          cb(null, result)

  getAttr: (cb) =>
    attr =
      mode: 0o100777,
      size: @size,
      nlink: 1,
      mtime: new Date(@mtime),
      ctime: new Date(@ctime)
    cb(0,attr)

  recursive: (start,end) =>
    file = @
    Fiber ()->
      path = pth.join(downloadLocation, "#{file.id}-#{start}-#{end}")
      unless exists(path).wait()
        unless downloadTree.has("#{file.id}-#{start}")
          downloadTree.set("#{file.id}-#{start}", 1)

          callback = (err,result) ->
            downloadTree.remove("#{file.id}-#{start}")
            unless err
              fs.writeFileSync(path,result)


          GFile.download(file.downloadUrl, start,end,@size,callback)
    .run()

  download: (start,end, readAhead, cb) ->
    #check to see if part of the file is being downloaded or in use
    file = @
    chunkStart = Math.floor((start)/GFile.chunkSize) * GFile.chunkSize
    chunkEnd = Math.min( Math.ceil(end/GFile.chunkSize) * GFile.chunkSize, file.size)-1 #and make sure that it's not bigger than the actual file
    nChunks = (chunkEnd - chunkStart)/GFile.chunkSize
    _download =  (cStart, cEnd,_cb) ->
      #if file chunk already exists, just download it
      #else download it
      Fiber () ->
        _cStart = Math.floor((cStart)/GFile.chunkSize)* GFile.chunkSize
        _cEnd = Math.min( Math.ceil(cEnd/GFile.chunkSize) * GFile.chunkSize, file.size)-1

        path = pth.join(downloadLocation, "#{file.id}-#{_cStart}-#{_cEnd}")
        if exists(path).wait()
          readSize = cEnd-cStart;
          buffer = new Buffer(readSize+1)
          fd = open(path, 'r').wait()
          # fd = fs.openSync(path,'r')
          read(fd,buffer,0,readSize+1, cStart - _cStart).wait()
          close(fd).wait()
          _cb(null, buffer)
        else
          unless downloadTree.has("#{file.id}-#{cStart}")
            downloadTree.set("#{file.id}-#{cStart}", 1)
            callback = (err, result)->
              if err
                _cb(err)
                return null
              downloadTree.remove("#{file.id}-#{cStart}")
              fs.writeFileSync(path,result)
              if result instanceof Buffer
                _cb(null, result.slice(cStart - _cStart, _cEnd - cEnd ))
              else
                _cb(result)
            GFile.download(file.downloadUrl, _cStart, _cEnd, file.size, callback)


          else
            fn = ->
              _download(cStart, cEnd,_cb)
            setTimeout fn, 1500
      .run()
    download = Future.wrap _download
    if nChunks < 1
      Fiber( ->
        fiber = Fiber.current
        fiberRun = ->
          fiber.run()
          return null

        #only read ahead if the start is within first 128kb of the chunk
        if readAhead
          if chunkStart <= start < chunkStart + 131072
            file.recursive( Math.floor(file.size / GFile.chunkSize) * GFile.chunkSize, file.size-1)
            file.recursive(chunkStart + i * GFile.chunkSize, chunkEnd + i * GFile.chunkSize) for i in [1..config.advancedChunks]

        #download chunks
        data = download(start,end)

        try
          data = data.wait()
        catch error #there might have been a connection error
          data = null
          logger.debug "debug", "failed to download chunk #{file.name}-#{start} - #{error.message}"
        if data == null
          cb()
          return
        cb( data )

      ).run()
    else if nChunks < 2
      end1 = chunkStart + GFile.chunkSize - 1
      start2 = chunkStart + GFile.chunkSize

      Fiber( ->
        fiber = Fiber.current
        data1 = download( start, end1)
        data2 = download( start2, end)

        try #check that data1 does not have any connection error
          data1 = data1.wait()
        catch error
          data1 = null

        try #check that data1 does not have any connection error
          data2 = data2.wait()
        catch
          data2 = null

        buf1 = Buffer.isBuffer(data1)
        buf2 = Buffer.isBuffer(data2)
        if buf1 and buf2
          cb(Buffer.concat([data1, data2]))
          return null
        else if buf1
          cb data1
          return null
        else
          cb( new Buffer(0) )
          return null

        return null
      ).run()
    else
      logger.log("error", "number of chunks greater than 2 - (#{start}-#{end})");
      buffer = new Buffer(0)
      cb(buffer)

module.exports.GFile = GFile
