
import urllib2
import threading
import re
from xml import sax

TWITTER_URL = "https://twitter.com/users/"
NUM_THREADS = 100 # Number of threads used to download data
NUM_IDS = 1000 # number of ids to download
START_ID = 284991007
END_ID = START_ID + NUM_IDS

## {{{ http://code.activestate.com/recipes/534109/ (r8)
def xml2obj(src):
    """
    A simple function to converts XML data into native Python object.
    """

    non_id_char = re.compile('[^_0-9a-zA-Z]')
    def _name_mangle(name):
        return non_id_char.sub('_', name)

    class DataNode(object):
        def __init__(self):
            self._attrs = {}    # XML attributes and child elements
            self.data = None    # child text data
        def __len__(self):
            # treat single element as a list of 1
            return 1
        def __getitem__(self, key):
            if isinstance(key, basestring):
                return self._attrs.get(key,None)
            else:
                return [self][key]
        def __contains__(self, name):
            return self._attrs.has_key(name)
        def __nonzero__(self):
            return bool(self._attrs or self.data)
        def __getattr__(self, name):
            if name.startswith('__'):
                # need to do this for Python special methods???
                raise AttributeError(name)
            return self._attrs.get(name,None)
        def _add_xml_attr(self, name, value):
            if name in self._attrs:
                # multiple attribute of the same name are represented by a list
                children = self._attrs[name]
                if not isinstance(children, list):
                    children = [children]
                    self._attrs[name] = children
                children.append(value)
            else:
                self._attrs[name] = value
        def __str__(self):
            return self.data or ''
        def __repr__(self):
            items = sorted(self._attrs.items())
            if self.data:
                items.append(('data', self.data))
            return u'{%s}' % ', '.join([u'%s:%s' % (k,repr(v)) for k,v in items])

        # JT: added "new" methods
        def keys(self): return self._attrs.keys()
        def iteritems(self): return self._attrs.iteritems()
        def popitem(self): return self._attrs.popitem()
        def pop(self, k, d=None): return self._attrs.pop(k, d)
        def items(self): return self._attrs.items()

    
    class TreeBuilder(sax.ContentHandler):
        def __init__(self):
            self.stack = []
            self.root = DataNode()
            self.current = self.root
            self.text_parts = []
        def startElement(self, name, attrs):
            self.stack.append((self.current, self.text_parts))
            self.current = DataNode()
            self.text_parts = []
            # xml attributes --> python attributes
            for k, v in attrs.items():
                self.current._add_xml_attr(_name_mangle(k), v)
        def endElement(self, name):
            text = ''.join(self.text_parts).strip()
            if text:
                self.current.data = text
            if self.current._attrs:
                obj = self.current
            else:
                # a text only node is simply represented by the string
                obj = text or ''
            self.current, self.text_parts = self.stack.pop()
            self.current._add_xml_attr(_name_mangle(name), obj)
        def characters(self, content):
            self.text_parts.append(content)

    builder = TreeBuilder()
    if isinstance(src,basestring):
        sax.parseString(src, builder)
    else:
        sax.parse(src, builder)
    return builder.root._attrs.values()[0]
## end of http://code.activestate.com/recipes/534109/ }}}



def get_user(content):
    user = xml2obj(content)
    return user
    

def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [ alist[i*length // wanted_parts: (i+1)*length // wanted_parts] 
             for i in range(wanted_parts) ]


def process_lines(lines, num):
    global counter
    global output_file
    global total
    global output_data
     
    opener = urllib2.build_opener()

    #lock = threading.RLock()
    for user_id in lines:
        
        #request_url = TWITTER_URL + user_id + ".json"
        request_url = TWITTER_URL + user_id
        #authenticated_url = get_authenticated_url(request_url)
        try:
            response = opener.open(request_url)      
            user = get_user(response.read())
        except Exception, e:
            print str(e)
            continue
        
        output_line = '"%s";"%s"\n' % (user_id, user['screen_name'])
        #lock.acquire()
        output_file.write(output_line)
        #output_data.append(output_line) # TODO: save to DB
        
        counter += 1
        #lock.release()


def main():
    global counter
    global output_file
    global total
    global output_data
    output_data = []
    output_file = open('twitter_threaded.csv', 'w')

    pool = []  # threads pool
    ids = [str(i) for i in range(START_ID, END_ID)]
    total = len(ids)

    counter = 0
    thread_num = 0
    for sub_list in split_list(ids, NUM_THREADS):
        thread = threading.Thread(target=process_lines, args=(sub_list, thread_num))
        thread.start()
        pool.append(thread)
        thread_num += 1
      
    for thread in pool:
        thread.join()  

    #output_file.writelines(output_data)
    output_file.close()
    print "FINISH: %s, %s" % (total, counter)


if __name__ == '__main__':
    
    main()
    