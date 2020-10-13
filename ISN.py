#------------------------------------------------------------------------------
# IS-BASED RETWEETS CASCADES
#------------------------------------------------------------------------------


import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt
import time
from tqdm import tqdm
from collectingData import get_exists, get_is_friend, get_quote_matrix, get_quote_users, get_replies_users, get_reply_matrix, get_retweet_list, get_retweet_matrix, get_retweets_users, get_tw_info, get_users_metrics
import json
import itertools

import multiprocessing as mp
from functools import partial

#----------------------------------------------------------------------------
#multiprocessing setup
cores = processes=mp.cpu_count()
cores = 1
#-------------------------------------------------------------------------------

RETWEET = 0
QUOTE = 1
COMMENT = 2

weights=[]
weights.append( ( 0.35, 1, 0.7 ) )

users_to_check_404=[]
last=True

root_tweet_index = 0
#list of tweets ID that retweeted the message:
root_tweetID=[] 
root_data="" #original tweet date
#saving folder:
folder = ''

max_history = 9600

root_tw_info={}

#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
        # networks links probabilities
#-----------------------------------------------------------------------------
#-----------------------------------------------------------------------------
def f_all_links(all_links, users_id_unique, result):
    users_link = {}
    index, value = all_links
    for u in users_id_unique:
        if len(result.loc[u]):
            dd=pd.DataFrame(result.loc[u]).T
            if len(dd)<2:
                dd.index=dd.to
                del(dd['to'],dd['from'])
                dd['probabilities']=1
                users_link [u]=dd
            else:
                dd=pd.DataFrame(result.loc[u]).groupby('to').sum()
                dd['probabilities']=dd[index]/dd[index].sum()
                users_link [u]=dd
    return users_link

def f_links(cascades, tweetsinfo, all_users_link, retwettatore, k):
    index, value = cascades
    if retwettatore not in list( value.keys() ):
        value=habitual_friend( value, retwettatore, k, all_users_link[index], index, tweetsinfo )
    return value

def parallel_runs(all_links, users_id_unique, result):
    par_work=partial(f_all_links, users_id_unique=users_id_unique, result=result)
    res = pool.map(par_work, all_links)
    return res

def links_parallel_runs(cascades, tweetsinfo, all_users_link, retwettatore, k):
    par_work=partial(f_links, tweetsinfo=tweetsinfo, all_users_link=all_users_link, retwettatore=retwettatore, k=k )
    cascades = pool.map(par_work, cascades)
    return cascades

def links(root_tweetID):
   
    all_info = {}
    all_info=get_retweet_list(root_tweetID[root_tweet_index])
    all_info['results'].append( {
        'content_id': str( root_tw_info['result']['content']['id'] ),
        'date': root_tw_info['result']['content_date'],
        'user_id': str( root_tw_info['result']['content']['author']['id'] )
    } )
    tweetsinfo=pd.DataFrame({'user_id':[l['user_id'] for l in all_info['results']], 'timestamp':[str(datetime.strptime(l['date'].split('.')[0], '%Y-%m-%dT%H:%M:%S')) for l in all_info['results']]})

    tweetsinfo=tweetsinfo.drop_duplicates('user_id')
    tweetsinfo=tweetsinfo.reset_index()
    
    #CLEAN
    del( all_info )
    
    users_info_df=pd.DataFrame(index=tweetsinfo['user_id'].drop_duplicates(), columns=[ '#friends', '#followers', 'favourite', 'status' ] )

    usersIDs_list=list(users_info_df.index)
    usersIDs_list_str = str(usersIDs_list).replace('\', \'',',').replace('\']','').replace('[\'','')

    for i in tqdm(range(0,len(tweetsinfo))):
        userID=tweetsinfo['user_id'].loc[i]
        is_call_ok = False
        user_info = {}
        while not is_call_ok:
            try:
                user_info=get_users_metrics(userID)
                is_call_ok = True
            except:
                is_call_ok = False
        
        if user_info['status']==200:
            users_info_df['#friends'].iloc[i]= user_info['friends_count']
            users_info_df['#followers'].iloc[i]=user_info['followers_count']
            users_info_df['favourite'].iloc[i]=user_info['favourites_count']
            users_info_df['status'].iloc[i]=user_info['statuses_count']


    matrix_ret = {}
    matrix_quotes = {}
    matrix_comments = {}

    is_matrix_ok = False
    while not is_matrix_ok:
        try:
            matrix_ret=get_retweet_matrix(usersIDs_list_str, root_data)
            is_matrix_ok = True
        except:
            is_matrix_ok = False
    is_matrix_ok = False
    while not is_matrix_ok:
        try:
            matrix_quotes=get_quote_matrix(usersIDs_list_str, root_data)
            is_matrix_ok = True
        except:
            is_matrix_ok = False
    is_matrix_ok = False
    while not is_matrix_ok:
        try:
            matrix_comments=get_reply_matrix(usersIDs_list_str, root_data)
            is_matrix_ok = True
        except:
            is_matrix_ok = False
    
    #-------------------------------------------------------------------------
    ret_pr=matrix_ret['matrix']
    del( matrix_ret )
    quot_temp=matrix_quotes['matrix']
    del( matrix_quotes )
    comments_temp=matrix_comments['matrix']
    del( matrix_comments )
    autore_ret=[]
    autore_quot=[]
    autore_comm=[]
    autore_originale_ret=[]
    autore_originale_quot=[]
    autore_originale_comm=[]
    count_ret=[]
    count_quot=[]
    count_comm=[]
    weighted_ret=[]
    weighted_quot=[]
    weighted_comm=[]
    for i in range(0,max( len(ret_pr), len(quot_temp), len(comments_temp))):
        if i < len( ret_pr ):
            autore_ret.append(ret_pr[i]['key']['autore'])
            autore_originale_ret.append(ret_pr[i]['key']['autore_originale'])
            count_ret.append(ret_pr[i]['docCount'])
        if i < len( quot_temp ):
            autore_quot.append(quot_temp[i]['key']['autore'])
            autore_originale_quot.append(quot_temp[i]['key']['autore_originale'])
            count_quot.append(quot_temp[i]['docCount'])
        if i < len( comments_temp ):
            autore_comm.append(comments_temp[i]['key']['autore'])
            autore_originale_comm.append(comments_temp[i]['key']['autore_originale'])
            count_comm.append(comments_temp[i]['docCount'])
    for c in range(0,len(weights)):
        weighted_temp_ret = []
        weighted_temp_quot = []
        weighted_temp_comm = []
        for i in range(0,max( len(ret_pr), len(quot_temp), len(comments_temp))):
            if i < len( ret_pr ):
                temp = users_info_df['#followers'].loc[ret_pr[i]['key']['autore_originale']]
                if temp == 0:
                    temp = 0.1
                weighted_temp_ret.append((ret_pr[i]['docCount']*weights[c][RETWEET])/temp)
            if i < len( quot_temp ):
                temp = users_info_df['#followers'].loc[quot_temp[i]['key']['autore_originale']]
                if temp == 0:
                    temp = 0.1
                weighted_temp_quot.append((quot_temp[i]['docCount']*weights[c][QUOTE])/temp)
            if i < len( comments_temp ):
                temp = users_info_df['#followers'].loc[comments_temp[i]['key']['autore_originale']]
                if temp == 0:
                    temp = 0.1
                weighted_temp_comm.append((comments_temp[i]['docCount']*weights[c][COMMENT])/temp)
        weighted_ret.append( weighted_temp_ret )
        weighted_quot.append( weighted_temp_quot )
        weighted_comm.append( weighted_temp_comm )
    
    ret_df=pd.DataFrame({'from': autore_ret, 'to':autore_originale_ret , 'count':count_ret}).join( pd.DataFrame( weighted_ret ).T )
    ret_df.index=ret_df['from']
    quotes_df=pd.DataFrame({'from': autore_quot, 'to':autore_originale_quot , 'count':count_quot}).join( pd.DataFrame( weighted_quot ).T )
    quotes_df.index=quotes_df['from']
    comments_df=pd.DataFrame({'from': autore_comm, 'to':autore_originale_comm , 'count':count_comm}).join( pd.DataFrame( weighted_comm ).T )
    comments_df.index=comments_df['from']
    del( autore_ret )
    del( autore_quot )
    del( autore_comm )
    del( autore_originale_ret )
    del( autore_originale_quot )
    del( autore_originale_comm )
    del( count_ret )
    del( count_quot )
    del( count_comm )
    del( weighted_ret )
    del( weighted_quot )
    del( weighted_comm )
    del( ret_pr )
    del( quot_temp )
    del( comments_temp )
    print( "DONE MATRIX" )
    
    frames = [ret_df, quotes_df,comments_df]
    result = pd.concat(frames)

     
    all_links=[{} for i in range (0,len(weights))]
    
    users_id_unique = list(set(result.index))

    print( "DOING ALL_LINKS" )
    if __name__ == '__main__':
        all_links = parallel_runs(enumerate(all_links), users_id_unique, result)
        del( frames )
        del( users_id_unique )
        del( result )

    print( "DONE ALL_LINKS" )

    return all_links, tweetsinfo
    
def findFriends( retweet_matrix, quotes_matrix,comments_matrix,users_info_df,weight_retweet,weight_quotes,weight_comments):
    users_link={}
    for user in users_info_df.index:
        try:
            keep_ret= retweet_matrix.loc[user].iloc[ retweet_matrix.loc[user].nonzero()[0]]
            try:
                keep_ret=keep_ret.drop([user], axis=0)
            except KeyError: 
                pass
            keep_quotes=quotes_matrix.loc[user].iloc[quotes_matrix.loc[user].nonzero()[0]]
            try:
                keep_quotes=keep_quotes.drop([user], axis=0)
            except KeyError: 
                pass
            
            keep_comm=comments_matrix.loc[user].iloc[comments_matrix.loc[user].nonzero()[0]]
            try:
                keep_comm=keep_comm.drop([user], axis=0)
            except KeyError: 
                pass     
            retweet_user=[(c,(weight_retweet*keep_ret[c])/users_info_df['#followers'].loc[c]) for c in keep_ret.index]
            quotes_user=[(c,(weight_quotes*keep_quotes[c])/users_info_df['#followers'].loc[c]) for c in keep_quotes.index]   
            comm_user=[(c,(weight_comments*keep_comm[c])/users_info_df['#followers'].loc[c]) for c in keep_comm.index] 
                           
        except IndexError:
            pass
            
        
        junto=pd.DataFrame(retweet_user+quotes_user+ comm_user)
        if len(junto)!=0:
            junto.columns=['userID','value']
            final=junto.groupby(junto['userID']).sum()
            final['probabilities']=final['value']/final['value'].sum()
            
            users_link[user]=final
    return users_link       


#------------------------------------------------------------------------------
#-----------------------------------------------------------------------------
#          CASCADE CREATION
#------------------------------------------------------------------------------
#-----------------------------------------------------------------------------

    
#livello zero:
def sort_by_time(tweetsinfo):
    tweetsinfo=tweetsinfo.sort_values(by=['timestamp'])

def levels(root_tweetID):
    #info del tweet
    root_timestamp=str(datetime.strptime(root_data.split('.')[0], '%Y-%m-%dT%H:%M:%S'))
    root_userID=root_tw_info['result']['content']['author']['id']
    cascades = []
    for c in range(0,len(weights)):
        cascades.append({'users_to_craw':[],'cascade_params':{'rtw_weight':weights[c][RETWEET],'qtd_weight':weights[c][QUOTE],'comm_weight':weights[c][COMMENT]},str(root_userID): {'user_id': str(root_userID), 'timestamp': root_timestamp,'Level': 0, 'retweedFrom':[{'user':str(root_userID),'prob':1}], 'maxFrom':{'user':str(root_userID),'prob':1},'type': 'root'}})
    return cascades
   
#double version: link to the oldest or newest retweet       
def last_retweet(  possibleChoices, times):        
    id_last=np.argmax(times)
    lastFriend=possibleChoices[id_last]
    return(lastFriend)
       
def first_retweet( possibleChoices, times):
    id_first=np.argmin(times)
    firstFriend=possibleChoices[id_first]
    return(firstFriend)
    
def find_the_link(cascade,retwettatore,k, cascata_index, tweetsinfo):
    from_id=retwettatore
    possibleChoices=[]
    status_error=[]
  
    if get_exists(from_id)['crawled'] is True: 
        for key in cascade.keys():
            risposta=get_is_friend(key,from_id)
            if risposta['status']==200:
                if risposta['is_friend'] is True:
                    possibleChoices.append(key)
            elif risposta['status']!=200:
                 status_error.append(key)
                 
        if len(possibleChoices)!=0:
            
            times=[cascade[iden]['timestamp'] for iden in possibleChoices]
            if last is False:
                friend=first_retweet(possibleChoices, times)
            else:
                friend=last_retweet(possibleChoices, times)
          
            try:
              
                cascade[retwettatore] = {'user_id': retwettatore, 'timestamp': tweetsinfo['timestamp'].loc[k],
                                        
                                         'retweedFrom': [{'user': friend, 'prob': (1 / len(possibleChoices))}],
                                         'maxFrom':{'user':friend,'prob': (1 / len(possibleChoices))}, 'type': 'friends_list'}
            except TypeError:
            
                cascade[retwettatore] = {'user_id': retwettatore, 'timestamp': tweetsinfo['timestamp'].loc[k],

                                         'retweedFrom': [{'user': friend, 'prob': (1 / len(possibleChoices))}],
                                         'maxFrom':{'user':friend,'prob': (1 / len(possibleChoices))}, 'type': 'friends_list'}

            if len(status_error)!=0:
                users_to_check_404.append(status_error)
            
           

        elif len(status_error)!=0 and len(possibleChoices)==0:
           
            users_to_check_404.append(status_error)
        elif len(status_error)==0 and len(possibleChoices)==0:
               cascade[retwettatore] = {'user_id': retwettatore, 'timestamp': tweetsinfo['timestamp'].loc[k],'Level': 'SP', 'retweedFrom': None,'maxFrom':{'user':None,'prob': 1}, 'probability': 1, 'type': 'friends_list'}
         
    
    elif get_exists(int(from_id))['crawled'] is False:

        cascade['users_to_craw'].append( from_id )
    
    return  cascade
                
def habitual_friend( cascade, retwettatore,k,users_link,cascata_index, tweetsinfo):
    location=tweetsinfo['timestamp'][tweetsinfo['user_id']==retwettatore].index
    retweet_time=tweetsinfo['timestamp'].loc[location].astype('str') [tweetsinfo['user_id']==retwettatore]
    
    if retwettatore in users_link.keys():
       possible_link=[c for c in users_link[retwettatore].index]
       probabilities_link=[c for c in users_link[retwettatore].probabilities]
       dates=[tweetsinfo[tweetsinfo['user_id']==c]['timestamp'] for c in possible_link]
       
       prob_keep=[]
       links_keep=[]
       links=[]
       max_p={'user':'','prob':0}
       for d in range(0,len(dates)):   
           
           if retweet_time[retweet_time.index[0]]>dates[d][dates[d].index[0]] and probabilities_link[d]!=0:
               prob_keep.append(probabilities_link[d])
               links_keep.append( possible_link[d])
               links.append( {'user':possible_link[d], 'prob':probabilities_link[d]} )
               if probabilities_link[d] > max_p['prob']:
                   max_p = {'user':possible_link[d], 'prob':probabilities_link[d]}


       if len(links_keep)!=0:
           cascade[retwettatore] = {'user_id': retwettatore, 'timestamp': retweet_time[retweet_time.index[0]], 'retweedFrom': links,'maxFrom': max_p,'probability': pd.DataFrame({'user_id':links_keep,'prob':prob_keep}).to_json(orient='index'), 'type': 'timeline'}

       elif len(links_keep)==0:
            cascade=find_the_link(cascade,retwettatore,k, cascata_index, tweetsinfo)
    elif retwettatore not in users_link.keys():
        cascade['users_to_craw'].append( retwettatore )
        cascade=find_the_link(cascade,retwettatore,k, cascata_index, tweetsinfo)
    return cascade

                       
def links_levels(cascades,tweetsinfo,all_users_link):
         for k in tqdm(range(0,len(list(tweetsinfo['user_id'])))):
      
            retwettatore = tweetsinfo[ 'user_id' ].iloc[ k ]
            for c in tqdm(range(0,len(all_users_link))):
                if retwettatore not in list( cascades[c].keys() ):
                    cascades[c]=habitual_friend( cascades[c], retwettatore, k, all_users_link[c], c, tweetsinfo )
                
         return cascades

def links_levels_parallelo(cascades,tweetsinfo,all_users_link):
    for k in tqdm(range(0,len(list(tweetsinfo['user_id'])))):
        retwettatore = tweetsinfo[ 'user_id' ].iloc[ k ]
        cascades = links_parallel_runs( enumerate(cascades), tweetsinfo,  all_users_link, retwettatore, k )
    return cascades

#------------------------------------------------------------------------------
#to run:
pool = None
if __name__ == '__main__':
    
    pool = mp.Pool(cores)

    print( "processors: ", cores )

    for i in range(0,len(root_tweetID)):

        root_tweet_index = i
        print( 'Cascade ' + root_tweetID[root_tweet_index] )

        users_to_check_404=[]
        last=True

        root_tw_info = {}
        is_call_ok = False
        while not is_call_ok:
            try:
                root_tw_info = get_tw_info( root_tweetID[root_tweet_index] )
                is_call_ok = True
            except:
                is_call_ok = False
        root_data = ''
        all_link = {}
        tweetsinfo = {}
        cascades = []
        if root_tw_info['status'] != 404:
            root_data = root_tw_info[ 'result' ][ 'content_date' ]

            all_link, tweetsinfo = links( root_tweetID )
            cascades=levels( root_tweetID )
            cascades=links_levels_parallelo( cascades, tweetsinfo, all_link )

       

            json.dump( cascades, open( folder + '\\cascata_' + str( root_tweetID[root_tweet_index] ) + '.json', 'w' ) )
        else:
            print( "TWEET NOT FOUND" )
    pool.close()
    print( 'DONE' )