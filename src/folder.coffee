class GFolder
  constructor: (@id, @parentid, @name, @ctime, @mtime, @permission, @children = []) ->

  getAttr: (cb)=>
    attr =
      mode: 0o40777,
      size: 4096 #standard size of a directory
      nlink: @children.length + 1,
      mtime: new Date(@mtime),
      ctime: new Date(@ctime)
    cb(0,attr)

module.exports.GFolder = GFolder
