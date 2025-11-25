class nodeLibrary:
    """Node library class to keep the node data for passing requests between functions
    """

    def __init__(self, listInput : list) -> None:
        self.targets, self.sources, self.nodes = self.findNodes(listInput)
        self.nodePaths = self.findNodeStructure(self.sources,self.nodes,self.targets,listInput)
        self.id2node = {nodeDict['id'] : nodeDict['name'] for nodeDict in self.nodePaths.values()}
        self.node2id = {nodeDict['name'] : nodeDict['id'] for nodeDict in self.nodePaths.values()}
 


    def findNodes(self,table1, table2 = None):
        tableEntries = [*table1,*table2] if table2 else table1
        targets = set(entry.get("Target name") for entry in tableEntries)
        sources = set(entry.get("Source name") for entry in tableEntries)
        nodes = targets & sources
        targets = targets - nodes
        sources = sources - nodes
        return targets,sources,nodes

    def findNodeStructure(self,sources,nodes,targets, entries):
        nodeStruc = {node : {'id' : idx, 'name' : node, 'lowestLevel' : 0, 'above' : set(), 'below' : set()} for idx, node in enumerate(sorted(nodes))}
        searchEntryDict = {f"{entry['Source name']} > {entry['Target name']}" : entry for entry in entries if entry['Source name'] not in sources and entry['Target name'] not in targets} #only node to node data is helpful
        #cuts down to only unique source --> target values
        searchEntries = [entry for entry in searchEntryDict.values()]
        idx = 0
        while True and idx < 10: #max idx as 10 as a safety measure
            changeMade = False
            for entry in searchEntries:
                src = entry['Source name']
                tgt = entry['Target name']
                #if a target is from a source, it must be at LEAST one level beneath. Another source may push it further
                if nodeStruc[tgt]['lowestLevel'] < nodeStruc[src]['lowestLevel'] + 1:
                    nodeStruc[tgt]['lowestLevel'] = nodeStruc[src]['lowestLevel'] + 1
                    changeMade = True
                nodeStruc[src]['below'].add(nodeStruc[tgt]['id'])
                nodeStruc[tgt]['above'].add(nodeStruc[src]['id'])
            if not changeMade:
                break
            idx += 1
            if idx == 9:
                print("Warning: node structure search has reached maximum index. Should not occur")
        print(f"maximum node depth [zero index]: {idx}")
        return nodeStruc