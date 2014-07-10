import numpy as np
from numpy import matlib

#this is the string distance calculation
from dedupe.distance.affinegap import normalizedAffineGapDistance as comparator

#Canonicalization###########################################################################

# takes in a list of attribute values for a field,
# evaluates the centroid using the comparator,
# & returns the centroid (i.e. the 'best' value for the field)
def getCentroid( attribute_variants, comparator ):
    n = len(attribute_variants)
    print n
    print "HOOO"
    # if all values were empty & ignored, return ''
    if n == 0:
        return ''
    if n == 1:
        return attribute_variants[0]
    dist_matrix = np.zeros([n,n])
    # this is a matrix of distances between all strings
    # populate distance matrix by looping through elements of matrix triangle
    for i in range (1,n):
        print i
        for j in range (0, i):
            dist = comparator(attribute_variants[i], attribute_variants[j])
            dist_matrix[i,j] = dist
            dist_matrix[j,i] = dist
            print i, j, dist
            print attribute_variants[i], attribute_variants[j]
            print "********"
    # find avg distance per string
    avg_dist = dist_matrix.mean(0)
    print avg_dist
    # find string with min avg distance
    
    min_dist_indices = np.where(avg_dist==avg_dist.min())[0]
    print min_dist_indices
    # if there is only one value w/ min avg dist
    if len(min_dist_indices)==1:
        centroid_index = min_dist_indices[0]
        return attribute_variants[centroid_index]
    # if there are multiple values w/ min avg dist
    else:
        return breakCentroidTie( attribute_variants, min_dist_indices )

# find centroid when there are multiple values w/ min avg distance (e.g. any dupe cluster of 2)
# right now this just selects the first among a set of ties
# TO-DO? set this up so that for strings, it breaks ties by selecting the longest string (would need to take in comparator)
def breakCentroidTie( attribute_variants, min_dist_indices ):
    return attribute_variants[min_dist_indices[0]]

#this is the string distance calculation
from dedupe.distance.affinegap import normalizedAffineGapDistance as comparator

# takes in a cluster of duplicates & data, returns canonical representation of cluster
# TO-DO: get this to take in data model, so that it knows data types, comparators
def getCanonicalRep( dupe_cluster, data_d):
    keys = data_d[0].keys()
    canonical_rep = dict()
    ####### TO-DO ############
    # comparator = 

    #loop through keys & values in data, get centroid for each key
    for key in keys:
        key_values = []
        for record_id in dupe_cluster :
            #ignore empty values (assume non-empty values always better than empty value for canonical record)
            if data_d[record_id][key] != '':
                key_values.append(data_d[record_id][key])
        canonical_rep[key] = getCentroid(key_values, comparator)
    return canonical_rep


#TESTING getCanonicalRep
#print "****TESTING get CanonicalRep****"
#dupe_cluster = clustered_dupes[0]
#print dupe_cluster
#canonical_rep_test = getCanonicalRep(dupe_cluster, data_d)
#print "CANONICAL REPRESENTATION: ", canonical_rep_test
#print "***************************"

addresses = ['2974 north clybourn avenue', '2905 n leavitt street', '2820 n leavitt st', '2974 n clybourn ave', '2905 n leavitt', '2820 n leavitt', '2974 n clybourn ave', '2905 n clybourn avenue', '2820 n. leavitt', '2974 n. clybourn', '2974 n. clybourn', '2905 n. leavitt', '2820 n. leavitt', '2974 north clybourn avenue', '2905 n. leavitt', '2974 n. clybourn', '2905 n. leavitt', '2905 n. leavitt', '2820 n leavitt st', '2905 n leavitt st', '2974 n clybourn ave', '2820 n leavitt st', '2905 n leavitt st', '2974 n clybourn ave']

# TESTING getCentroid
print "****TESTING getCentroid****"
attributeList = ["a","ab", "ab", "ab", "abc"]
attributeList = ['mary crane center', 'mary crane center north', 'mary crane league - mary crane - west', 'mary crane league mary crane center (east)', 'mary crane league mary crane center (north)', 'mary crane league mary crane center (west)', 'mary crane league - mary crane - east', 'mary crane family and day care center', 'mary crane west', 'mary crane center east', 'mary crane league mary crane center (east)', 'mary crane league mary crane center (north)', 'mary crane league mary crane center (west)', 'mary crane league', 'mary crane', 'mary crane east 0-3', 'mary crane north', 'mary crane north 0-3', 'mary crane league - mary crane - west', 'mary crane league - mary crane - north', 'mary crane league - mary crane - east', 'mary crane league - mary crane - west', 'mary crane league - mary crane - north', 'mary crane league - mary crane - east']
print attributeList
centroid = getCentroid (attributeList, comparator)
print "CENTROID: ", centroid
print "***************************"

#############################################################################################