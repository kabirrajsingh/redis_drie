import redis,warnings

def get_formatted_output(setto,zfill_len=0):
  if len(setto)==0:
    return "EMPTY"

  return ','.join(sorted(list(map(lambda x:str(x).zfill(zfill_len),set(setto)))))

# ALPH_LIST=[chr(i) for i in range(33,123)]
ALPH_LIST=list("0123456789")+[chr(i) for i in range(65,91)]

def convert_to_base_N(num:int,base:int=len(ALPH_LIST)):
  if isinstance(num,str):
    num=int(num)
  
  if num==0:
    return ALPH_LIST[0]
  ans=''
  while num>0:
    ans+=ALPH_LIST[num%base]
    num//=base
  return ans[::-1]

# globalNodeNumber=1

class RedisClient():
  def __init__(self,host:str='localhost',port:int=6379,db:int=0,decode_responses:bool=True):
    self.client=redis.Redis(host=host, port=port, db=db, decode_responses=decode_responses)

  def copy_from_another_redis_db(self,new_redis_client,trieId:int):
    self.flushDB()
    self.setAttrToDB("trieId",trieId)
    allKeys=new_redis_client.client.keys()
    for key in allKeys:
      if key=="trieId":
        self.setAttrToDB("trieId",trieId)
      elif key=="testfilename":
        continue
      elif key=="company_trie_head":
        self.setAttrToDB("company_trie_head",self.convert_by_trieid(new_redis_client.getAttrFromDB(key),trieId))
      elif key=="pincode_trie_head":
        self.setAttrToDB("pincode_trie_head",self.convert_by_trieid(new_redis_client.getAttrFromDB(key),trieId))
      elif key=="globalNodeNumber":
        self.setAttrToDB("globalNodeNumber",new_redis_client.getAttrFromDB(key))
      else:
        if key[1]=="1": # hashdict
          tempDict=new_redis_client.client.hgetall(key)
          for dictKey in tempDict.keys():
            if dictKey=="parent" and tempDict[dictKey]:
              tempDict[dictKey]=self.convert_by_trieid(tempDict[dictKey],trieId)
            elif dictKey=="num_word":
              continue
            elif dictKey=="data":
              continue
            else:
              tempDict[dictKey]=self.convert_by_trieid(tempDict[dictKey],trieId)

          self.client.hset(self.convert_by_trieid(key),mapping=tempDict)

        elif key[1]=="2": #set
          tempSet=set(self.convert_by_trieid(s,trieId) for s in new_redis_client.client.smembers(key))
          self.addSkipTrieConnOneWay(self.convert_by_trieid(key),tempSet)
        else:
          raise Exception(f"Unsupported key type: {key}")

    

  def convert_by_trieid(self,node:str,trieId:int):
    if node.startswith(("1","2","3","4","5","6","7","8","9","0")):
      return str(trieId)+node[1::]

  def createRedisNode(self,trieId:int,data:str,parent:str,num_word:int,trans:dict=None,skip_conn:set=None):
    globalNodeNumber=self.client.get(f"globalNodeNumber{trieId}")
    
    if not globalNodeNumber:
      self.client.set(f"globalNodeNumber{trieId}",1)
      globalNodeNumber="1"

    nodeIdWithTrans=str(trieId)+'1'+convert_to_base_N(globalNodeNumber)
    nodeIdWithSkipConn=str(trieId)+'2'+convert_to_base_N(globalNodeNumber)
    
    self.client.hset(nodeIdWithTrans,"data",data)
    self.client.hset(nodeIdWithTrans,"num_word",num_word)
    
    self.client.incrby(f"globalNodeNumber{trieId}",1)
    
    if parent:
      self.client.hset(nodeIdWithTrans,"parent",parent)
      self.client.hset(parent,data,nodeIdWithTrans)
    if trans:
      for k,v in trans.items():
        self.client.hset(nodeIdWithTrans,k,v)
    

    # the idea is that the skipConnId will store all the transIds that it points to. This is done because skipConnIds are abstract things and are needed less.
    if skip_conn and len(skip_conn)>0:
      warnings.warn('can be potentially too many args due to the list expansion')
      self.client.sadd(nodeIdWithSkipConn,*skip_conn) # can be potentially too many args
      for node in skip_conn:
        self.client.sadd(self.transformTransToSkipNode(node),nodeIdWithTrans)

    return nodeIdWithTrans
  
  def setAttrToDB(self,key,val):
    self.client.set(key,val)
  def getAttrFromDB(self,key):
    return self.client.get(key)
  def delAttrFromDB(self,key):
    self.client.delete(key)

  def transformTransToSkipNode(self,transNode:str):
    if isinstance(transNode,list):
      skipNode=list(node[0]+'2'+node[2::] for node in transNode)
    elif isinstance(transNode,set):
      skipNode=set(node[0]+'2'+node[2::] for node in transNode)
    elif isinstance(transNode,str):
      skipNode=transNode[0]+'2'+transNode[2::]
    else: 
      raise Exception(f"Unsupported Type {type(transNode)} in transformTransToSkipNode")

    return skipNode

  def getAllNextNodes(self,nodeId:str):
    fullResult=self.client.hgetall(nodeId)
    if not fullResult:
      return dict()
    
    for k in ['data','parent','num_word']:
      if k in fullResult:
        del fullResult[k]
    
    return fullResult

  def getNextNode(self,nodeId:str,transChar:str):
      return self.client.hget(nodeId,transChar)

  def getAttrForTransNode(self,nodeId:str,attr:str):
    return self.client.hget(nodeId,attr)
  
  def delAttrForTransNode(self,nodeId:str,attr:str):
    self.client.hdel(nodeId,attr)
  
  def setAttrForTransNode(self,nodeId:str,key:str,value:str):
    #supposed to be a hashset only
    self.client.hset(nodeId,key,value)

  def addSkipTrieConnBothWay(self,company_leaf:str,pincode_leaf:str):

    self.client.sadd(self.transformTransToSkipNode(company_leaf),pincode_leaf)
    self.client.sadd(self.transformTransToSkipNode(pincode_leaf),company_leaf)

  def addSkipTrieConnOneWay(self,leaf1:str,leaf2List:list[str]):
    if len(leaf2List)!=0:
      self.client.sadd(self.transformTransToSkipNode(leaf1),*leaf2List)

  def getAllSkipConns(self,nodeId:str):
    nodeId=self.transformTransToSkipNode(nodeId)
    return self.client.smembers(nodeId)

  def getAllSkipConnsIntersections(self,nodeIds:list[str]):
    nodeIds=self.transformTransToSkipNode(nodeIds)
    return self.client.sinter(nodeIds)
  
  def setAllSkipConns(self,nodeId:str,skipConns:list[str]):
    nodeId=self.transformTransToSkipNode(nodeId)

    self.client.delete(nodeId)
    if len(skipConns)!=0:
      self.client.sadd(nodeId,*skipConns)

  def removeFromSkipConns(self,nodeId:str,skipConns:list[str]):
    nodeId=self.transformTransToSkipNode(nodeId)
    if len(skipConns)!=0:
      self.client.srem(nodeId,*skipConns)

  def flushDB(self):
    self.client.flushdb()
