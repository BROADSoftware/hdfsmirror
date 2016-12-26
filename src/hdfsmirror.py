import sys
import os
import argparse
import lib.misc as misc


def buildLocalTree(rroot):
    tree = {}
    if rroot.endswith("/"):
        rroot = rroot[:-1]
        tree['slashTerminated'] = True
    else :
        tree['slashTerminated'] = False
    tree['rroot'] = rroot
    prefLen = len(rroot) + 1
    fileList = []
    for root, _, files in os.walk(rroot, topdown=True, onerror=None, followlinks=False):
        for fileName in files:
            f = os.path.join(root, fileName)
            f = f[prefLen:]
            fileList.append(f)
    fileList.sort()
    tree['files'] = fileList
    return tree
    
def buildLocalTree2(src):
    for root, dirs, file in os.walk(src, topdown=True, onerror=None, followlinks=False):
        print root, dirs, file


def main():
    mydir =  os.path.dirname(os.path.realpath(__file__)) 
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', required=True)
    parser.add_argument('--dst', required=True)
    parser.add_argument('--put', action='store_true')
    parser.add_argument('--get', action='store_true')

    param = parser.parse_args()
    if param.put and param.get:
        misc.ERROR("Only one of --put or --get must be set")
    if not param.put and not param.get:
        misc.ERROR("One of --put or --get must be set")

    src = param.src
    dst = param.dst
    
    if(param.put):
        if not os.path.isdir(src):
            misc.ERROR("{0} must be an existing folder".format(src))
        localFiles = buildLocalTree(src)
    else:
        if not os.path.isdir(dst):
            misc.ERROR("{0} must be an existing folder".format(dst))
        localFiles = buildLocalTree(dst)
        

    misc.pprint(localFiles)
    

if __name__ == '__main__':
    sys.exit(main())
