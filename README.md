# Company Recommendation

Suppose there is a Graph of company where node represents company name (eg. C1, C2, C3 …) and
edge represents exiting business between two companies. Know, a company wants to expand its
business. They are looking for MAP REDUCE application to find two hop highest distinct distance
companies which have no direct connection between them.
Eg.
For C1, below are the two hop distance
C1-C2-C4 , C1-C3-C4 , C1-C2-C3 , C1-C3-C2 , C1-C5
Out of this C1-C2-C3 , C1-C3-C2 and C1-C5 have direct business with C1, so these are discarded
Remaining C1-C2-C4 and C1-C3-C4 are two distinct path from C1 to C4
So we will recommend C4 to C1
Output will be like [C4:2], =>C1 Where 2 represents distinct path

Note:
1. Recommend only top two companies , rest will be discarded
2. If there is only one two hop distance company, output will be only one
eg. In case of C1, [C4:2]
3. Output order should be sorted by highest to lowest distinct path
Eg. For C8 we have following distinct path C6 (2), C2 (1) and C3(1)
[C6:2],[C2:1], =>C8
4. If same distinct path eg.
In case of C8, ( eg. C2 (1) or C3(1)) , output any one , both are valid
[C6:2],[C2:1], =>C8
[C6:2],[C3:1], =>C8

    ----------[C1]-----------------[C5]
    |            |
    |            |
    [C2]---------[C3]         
    |          |
    |------[C4]------------[C8]----------[C7] 
              |                          |
              |                          |
              |-------------------------[C6]


              
              
              
INPUT:

C1:C2,C3,C5

C2:C1,C4,C3

C3:C1,C4,C2

C4:C2,C3,C6,C8

C6:C4,C7

C5:C1

C7:C6,C8

C8:C7,C4

OUTPUT:

[C4:2], =>C1

[C8:1],[C5:1], =>C2

[C8:1],[C5:1], =>C3

[C7:2],[C1:2], =>C4

[C3:1],[C2:1], =>C5

[C8:2],[C2:1], =>C6

[C4:2], =>C7

[C6:2],[C2:1], =>C8


