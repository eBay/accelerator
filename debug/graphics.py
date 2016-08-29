import subprocess




def plot(data):

    s = 'digraph pelle {\n'
    for key, val in data.items():
        stajl = ''
        if val['method'].startswith('index'):
            stajl = ',style=filled,color=lightpink'
        if val['method'].startswith('subset'):
            stajl = ',style=filled,color=lightblue'
        s += '\"%s\" [label=\"%s\n%s\n%s\"%s]' % (key, key, val['method'], val['caption'],stajl)

    for key, val in data.items():
        for next in val['dep'].keys():
            s += '\"%s\" -> \"%s\"' % (key, next)
    s += '}'
    print(s)




"""
            import StringIO
            F = StringIO.StringIO()
            F = export_graphviz(model, out_file=F)
            F.seek(0)
            G = open( filename,'wb')
            for line in F:
                if 'label="error' in line:
                    line = line.replace('] ;',', fillcolor="#cccccc", style="filled"] ;',1)
                else:
                    for x in range(len(filtered_names)):
                        line = line.replace('X[%d]'%x, names[x])
                G.write(line)
            G.close()
        else:
            F = open( filename,'wb')
            F = export_graphviz(model, out_file=F)
            F.close()
        f = subprocess.Popen(['dot',filename,'-T','png','-o',filename.replace('.dot','.png')])
        f.wait()
"""
