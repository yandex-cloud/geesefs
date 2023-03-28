# Delve (https://github.com/go-delve/delve) script to dump all GeeseFS file buffer states
def main():
    # Like inodes = eval(None, "fs.inodes").Variable in the Flusher goroutine
    inodes = None
    for g in goroutines().Goroutines:
        s = stacktrace(g.ID, 100)
        i = 0
        for l in s.Locations:
            if l.Location.Function.Name_.endswith('.Flusher'):
                inodes = eval({ "GoroutineID": g.ID, "Frame": i }, "fs.inodes").Variable
                break
            i = i+1
    if not inodes:
        return
    for id in inodes.Value:
        inode = inodes.Value[id]
        if len(inode.buffers) > 0:
            print(inode.Name)
            for buf in inode.buffers:
                print(buf.offset, " ", buf.length, " ", buf.state, " ", buf.loading, " ", buf.zero, " ", buf.recency, " ", buf.dirtyID, " ", buf.data != None)
