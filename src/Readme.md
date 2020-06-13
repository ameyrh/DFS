1.  File runs from DFSUI
2.  The Public Private Key pair are generated exclusively for this module.
3.  All IPs are either local host or a specified ip change them as per your need.
4.  Segmentation can be implemented using file channels rather than byte arrays that would be faster.
5.  Refer isec module for model code.(isec Module is courtesy Mr Sarthak Bisht)
6.  XML parsing has been done using StAX cursor API
7.  On uploading the same file twice there is an error. To get rid of it search the index on encountering the same path delete the existing entries then only create new index entries.
8.  Ideally have two machines to test the code. Or else create a virtual machine and test the code.
9.  Port number can be dynamically alloted. Look up isec.
