from scripts.commonValues import maxRecursion


class nodeLibrary:
    """Node library class to keep the node data for passing requests between functions
    """

    def __init__(self, listInput : list) -> None:
        self.targets, self.sources, self.nodes = self.findNodes(listInput)
        self.badNodes = set()
        self.nodePaths = self.findNodeStructure(self.sources,self.nodes,self.targets,listInput)
        self.id2node = {nodeDict['id'] : nodeDict['name'] for nodeDict in self.nodePaths.values()}
        self.node2id = {nodeDict['name'] : nodeDict['id'] for nodeDict in self.nodePaths.values()}
        self.node2Funds = self.findNode2Funds(listInput, self.nodes)
        self.lowNodes = [e['Target name'] for e in listInput if e['Target name'] in self.nodes and e['Source name'] in self.nodes] #Nodes that are targets of other nodes
 

    def findNode2Funds(self,tableEntries,nodes):
        node2Funds = {node : set() for node in (*nodes,'None')}
        for entry in tableEntries:
            src = entry['Source name']
            if src in nodes:
                node2Funds[src].add(entry['Target name'])
            elif src in self.sources: #investors direct data. No node
                node2Funds['None'].add(entry['Target name'])
        searching = True
        loopIdx = 0
        while searching and loopIdx < 10:
            searching = False 
            loopIdx += 1
            if loopIdx == 10:
                print("WARNING: node2Funds build iteration reached maximum depth")
            for node, targets in ([n,ts] for n,ts in node2Funds.items() if any(t in nodes for t in ts)): #iteratively assign any node's targets to the node's source
                searching = True #turns back on if anything found to alter
                tCopy = targets.copy()
                for target in (t for t in tCopy if t in nodes):
                    node2Funds[node].remove(target)
                    node2Funds[node].update(node2Funds[target])
        return node2Funds
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
        while True and idx < maxRecursion: #max idx as 10 as a safety measure
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
            if idx == maxRecursion - 1:
                print("Warning: node structure search has reached maximum index. Should not occur")
        deleteKeys = [node for node in nodeStruc if len(nodeStruc[node]['below'].intersection(nodeStruc[node]['above'])) > 0]
        for dKey in deleteKeys:
            print(f'WARNING: Deleting the following node for circular ownership: {dKey}')
            nodeStruc.pop(dKey)
            self.nodes.remove(dKey)
            self.badNodes.add(dKey)
        print(f"maximum node depth [zero index]: {idx}")
        return nodeStruc