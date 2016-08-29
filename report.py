import os
import string
import time

class report():
    def __init__(self, globdict, stdout=False):
        self.stdout = stdout
        self.s = ''
        self.line()
        self.println( '[%s]                    [jobnum:%s]' % (
                time.strftime('%a, %d %b %Y %H:%M:%S +0000',time.gmtime()), globdict['JOBID']))
        self.line()
        self.println('Method \"%s\" report.' % globdict['METHOD'])
        caption = globdict['CAPTION']
        if caption:
            self.println('Caption \"%s\"' % caption)
        self.line()
        options = globdict['OPTIONS']
        if options:
            self._options(options)
        self.line()
    # def __init__(self, directory, name=''):
    #     self.F = open(os.path.join(directory,'report.txt'),'wb')
    #     self.line()
    #     self.println( '[%s]                    [jobnum:%s]' % (
    #             time.strftime('%a, %d %b %Y %H:%M:%S +0000',time.gmtime()),os.path.basename(directory)))
    #     self.line()
    #     if name:
    #         self.println('Report: %s'%name)
    #         self.line()

    def line(self):
        self.s += '-'*80 + '\n'
#        print >> self.F, '-'*80

    def println( self, string ):
        self.s += string + '\n'
#        print >>self.F, string

    def write( self, string ):
        self.s += string
#        self.F.write( string )

    def printvec( self, vec, columns ):
        spacing = 80//columns-6
        for ix,x in enumerate(vec):
            self.write( '  %3d %s' %(ix,string.ljust(x,spacing)) )
            if ix%columns==(columns-1):  self.write('\n')
        if ix%columns==0:  self.write('\n')

    def options(self):
        print "REPORT,  use of OPTIONS is depreciated, call constructor with globals() instead!"
        exit(1)

    def _options( self, optionsdict, title='Options' ):
        if not optionsdict:  return
        self.println(title)
        maxlen = max([len(x) for x in optionsdict.keys()])
        for x,y in optionsdict.iteritems():
            # NB:  type checking!
            if type(y)==list:
                self.println('  %s :'%(string.ljust(x,maxlen)))
                for t in y:
                    self.println('  %s   %s'%(' '*maxlen,t))
            else:
                self.println("  %s : %s "%(string.ljust(x,maxlen),y))

    def close(self):
        self.line()
        with open('report.txt', 'wb') as F:
            F.write(self.s)
        if self.stdout:
            print self.s



class newreport(report):
    def __init__(**args):
        print "NEWREPORT GONE!"
    
