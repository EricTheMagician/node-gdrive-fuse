pth = require 'path'
fs = require 'fs-extra'
hashmap = require( 'hashmap' ).HashMap
Fibers = require 'fibers'
Future = require 'fibers/future'

######################################
######### Setup File Config ##########
######################################

config = fs.readJSONSync 'config.json'

#download location
downloadLocation = pth.join config.cacheLocation, 'download'
fs.ensureDirSync downloadLocation

#upload location
uploadLocation = pth.join config.cacheLocation, 'upload'
fs.ensureDirSync downloadLocation

downloadTree = new hashmap()

######################################
######### Wrap fs functions ##########
######################################

writeFile = Future.wrap(fs.writeFile)
open = Future.wrap(fs.open,2)
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

  # @recursive:  (client,file,rStart, rEnd) ->
  #   rEnd = Math.min( Math.ceil(rEnd/GFile.chunkSize) * GFile.chunkSize, file.size)-1
  #   basename = file.bitcasaBasename
  #   if (rEnd + 1) <= file.size and rEnd > rStart
  #     parentPath = client.bitcasaTree.get(pth.dirname(file.bitcasaPath))
  #     filePath = pth.join(parentPath,file.name)
  #     cache = pth.join(client.downloadLocation,"#{basename}-#{rStart}-#{rEnd}")
  #     Fiber ->
  #       unless client.exists(cache)
  #         unless client.downloadTree.has("#{file.bitcasaBasename}-#{rStart}")
  #           client.downloadTree.set("#{file.bitcasaBasename}-#{rStart}",1)
  #           _callback = ->
  #             client.downloadTree.remove("#{file.bitcasaBasename}-#{rStart}")
  #           _fn = ->
  #             client.download(client, file, file.bitcasaPath, file.name, rStart,rEnd,file.size, false , _callback)
  #           setImmediate _fn
  #     .run()

  @download = (url, saveLocation, start,end, cb ) ->
    rest.get url, {
      decoding: "buffer"
      timeout: 300000
      headers:
        "Authorization": "Bearer #{config.accessToken.access_token}"
        "Range": "bytes=#{start}-#{end}"
    }
    .on 'complete', (result, response) ->
      if result instanceof Error
        _cb(result)
      else
        _cb(null, result)


  download: (start,end, readAhead, cb) ->
    #check to see if part of the file is being downloaded or in use
    file = @
    chunkStart = Math.floor((start)/GFile.chunkSize) * GFile.chunkSize
    chunkEnd = Math.min( Math.ceil(end/GFile.chunkSize) * GFile.chunkSize, file.size)-1 #and make sure that it's not bigger than the actual file
    nChunks = (chunkEnd - chunkStart)/GFile.chunkSize
    download = Future.wrap (cStart, cEnd,_cb) ->
      #if file chunk already exists, just download it
      #else download it
      Fiber () ->
        path = pth.join(downloadLocation, "#{file.id}-#{Math.floor((cStart)/GFile.chunkSize) * GFile.chunkSize}-#{ Math.min( Math.ceil(cEnd/GFile.chunkSize) * GFile.chunkSize, file.size)-1}")
        if exist(path).wait()

          readSize = end - start;
          buffer = new Buffer(readSize+1)
          fd = open(path, 'r').wait()
          read(fd,buffer,0,readSize+1, start-chunkStart).wait()
          _cb(null, buffer)
          close(fd)
        else
          unless downloadTree.has("#{file.name}-#{cStart}")
              downloadTree.set("#{file.name}-#{cStart}", 1)
              callback = (err, result)->
                if err
                  return _cb(err)
                downloadTree.remove("#{file.name}-#{cStart}")
                fs.writeFileSync(path,result)
                #TODO: Splice buffer
                _cb(null, result)


          else
            fn = ->
              download(cStart, cEnd,_cb)
            setTimeout fn, 1500
      .run()
    if nChunks < 1
      Fiber( ->
        fiber = Fiber.current
        fiberRun = ->
          fiber.run()
          return null

        #only read ahead if the start is within the start of the chunk
        if readAhead
          if chunkStart <= start < chunkStart + 131072
            BitcasaFile.recursive(client,file, Math.floor(file.size / GFile.chunkSize) * GFile.chunkSize, file.size)
            BitcasaFile.recursive(client,file, chunkStart + i * GFile.chunkSize, chunkEnd + i * GFile.chunkSize) for i in [1..client.advancedChunks]

        #download chunks
        data = download(start,end)

        try
          data = data.wait()
        catch error #there might have been a connection error
          data = null
          client.logger.debug "debug", "failed to download chunk #{file.name}-#{start} - #{error.message}"
        if data == null
          cb()
          return
        cb( data.buffer, data.start, data.end )

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

        if data1 == null or data1.buffer.length == 0
          cb( new Buffer(0), 0, 0)
          return
        buffer1 = data1.buffer.slice(data1.start, data1.end)

        if data2 == null or data2.buffer.length == 0
          #since buffer1 is still good, just return that
          cb( buffer1, 0, data1.buffer.length )
          return
        buffer2 = data2.buffer.slice(data2.start, data2.end)

        buffer = Buffer.concat([buffer1, buffer2])
        cb( buffer, 0, buffer.length )
        return
      ).run()
    else
      client.logger.log("error", "number of chunks greater than 2 - (#{start}-#{end})");
      buffer = new Buffer(0)
      r =
        buffer: buffer
        start: 0
        end: 0
      cb(null, r)

module.exports.GFile = GFile
