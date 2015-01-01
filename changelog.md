##v0.6.3
fix bug #28, #34. When refreshing access token, use the auth client from client.coffee

##v0.6.2
fix bug #25 - file uploads were not being resumed properly on load.

##v0.6.1
fixes bug #31 - maximum inode value was not being calculated properly.

##v0.6.0
* Fixed memory leak issues. Switched from the restler package to the request package.
* With the request package, piping files is now possible and is used for both file uploads and downloads. As a result, the upload chunk size is now obsolote and not used.
* Switched out fuse4js for fusejs packages. fusejs is truly asynchronous and provides a major performance boost over fuse4js