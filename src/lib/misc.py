
import pprint


prettyPrinter = pprint.PrettyPrinter(indent=2)

def pprint(obj):
    prettyPrinter.pprint(obj)
                

def ERROR(err):
    if type(err) is str:
        message = err
    else:
        message = err.__class__.__name__ + ": " + str(err)
    print "* * * * ERROR: " + str(message)
    #raise Exception("xx")
    exit(1)


