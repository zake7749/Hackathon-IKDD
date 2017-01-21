# IKDD Hackathon

透過 **Spark** 以 Map Reduce 於叢集上進行 NBA 賽局資料的並行分析。

## Environment

* python 2.7
* Apache Spark 2.1.0
* Hadoop 2.7

## DataSet
小量資料集採用 Kaggle 上的 [NBA shot logs](https://www.google.com.tw/search?q=Kaggle+NBA+shot+log&ie=utf-8&oe=utf-8&gws_rd=cr&ei=mTSDWMfNHojp0ASNuoCIAg)
### 重要欄位簡述
* GAME_ID – 比賽ID
* MATCHUP – 比賽隊伍
* LOCATION – 主場(H)/客場(A)
* W – 贏(W)或輸(L)
* DRIBBLES – 帶球時間
* SHOT_DIST – 射籃距離
* SHOT_RESULT – 是否投中 (made/missed)
* CLOSEST_DEFENDER – 射籃時最近的防守球員
* CLOSE_DEF_DIST – 射籃時與最近防守球員的距離
* player_name – 該球射籃球員
* FINAL_MARGIN – 比賽結束分差
* SHOT_NUMBER – 某球員在該比賽第幾次射籃
* PERIOD – 第幾節
* TOUCH_TIME – 球員一拿到球到出手射籃的時間
* PTS_TYPE – 2 or 3分球

## Problem 1
列出資料中，所有出現過球員的命中率。
( 命中率僅列出小數點後兩位 eg. 0.88，並由大到小排序 )

程式名稱 : Hit_Rate.language

輸出名稱 : Hit_Rate.txt

輸出範例 : brian roberts 0.77


## Problem 2
找出每位球員必定進球且進球數最多的射籃距離範圍。
( 如果有球員找出多個答案，請將所有結果列出，或者該位球員沒有範圍答案，則範圍結果輸出no，請參考下方輸出格式 )
```
格式1 – 多種結果(若有多種範圍結果則以空格為分隔繼續列出結果):
<球員名稱><空格><範圍結果1><空格><範圍結果2>...
範例： brian roberts 3.5-7.8 11.2-15.6


格式2 – 一種結果:
<球員名稱><空格><範圍結果1>
範例： brian roberts 3.5-7.8

格式3 – 沒有結果:
<球員名稱><空格><範圍結果1>
範例： brian roberts no
```

## Problem 3

列出每位球員只要在比賽中拿到幾分，該隊員所屬隊伍就必定贏球。
( 如果有球員是沒答案的，則分數輸出no )
---------------------------------------------------------------------
下方列表舉例球員a，與其它隊伍的比賽輸贏及球員a該場比賽的得分

|<與某對比賽>|<輸or贏>|<該球員得分>|
|-----------|-------|----------|
|Situation 1 | Situation 2 | Situation 3 |
|Team A  Win 20 | Team A Win 20 | Team A Lose 20 |
|Team B  Win 19 | Team B Lose 19 | Team B Win 19 |
|Team C Win 18 | Team C Win 18 | Team C Win 18 |
|Team D Win 17 | Team D Win 17 | Team D Win 17 |
|Team E Lose 16 | Team E Win 16 | Team E Win 16 |
|Team F Win 15 | Team F Win 15 | Team F Win 15 |

Result 1 : 拿 17 分必定贏球
Result 2 : 拿 20 分必定贏球
Result 3 : 沒答案

程式名稱 : win_point.language

輸出名稱 : win_point.txt

輸出範例 : brian roberts 20 or brian roberts no

輸出格式 : <球員名稱><空格><分數>
```
<球員名稱1> <分數>
<球員名稱2> <分數>
```
## Problem 4

根據資料中的射籃距離，找進球數且命中率最高的射籃距離範圍。
( 若計算出的答案超過1個，請將答案全部列出 )

程式名稱：top_range.language

輸出名稱 : top_range.txt

輸出格式 (可能有兩種情況，如下)

```
格式1 – 多種結果:
<範圍結果1><空格><範圍結果2>...
範例： 3.5-7.8 10.0-12.5
＊若有多種範圍結果則以空格為分隔繼續列出結果
格式2 – 一種結果:
<範圍>
範例： 3.5-7.8
輸出格式 : <範圍><空格><範圍>…
<範圍>…
```
