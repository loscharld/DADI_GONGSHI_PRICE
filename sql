select  f.DD_CATE_ID DD_BRAND_ID,aa.MODEL_BRAND,aa.model_cate_name,
aa.commonname,aa.ORIGINALCODE,aa.PARTSTANDARDCODE,aa.CHANGFANGJIA,aa.UNITPRICE ,aa.CHGCOMPSET,aa.IS4S,aa.NEW_IS4S,aa.dc,
aa.lossapprovalid,aa.REGISTNO,aa.VERIFYFINALDATE
      from (select B.CHGCOMPSET, A.dc,
      CASE  WHEN B.REFPRICE1 IS NULL THEN
                              decode(A.peacecaseflag, 'N', B.upperlimitprice, B.REFPRICE1)
                             ELSE B.REFPRICE1 END changfangjia,
      B.reFunitPrice, B.LOCPRICE3,B.offerprice,b.ORIGINALCODE,b.PARTSTANDARDCODE,
      b.LOSSAPPROVALID as b_LOSSAPPROVALID , b.posid,b.posname,b.commonid, b.commoncode,b.commonname,a.MODEL_BRAND,b.UNITPRICE,a.model_cate_name,
      a.IS4S,a.NEW_IS4S,a.lossapprovalid,a.REGISTNO,a.VERIFYFINALDATE
              FROM (select * from lb_PRPCARINFO where  MODEL_BRAND ='上汽通用五菱' or MODEL_BRAND ='长安福特') A
             INNER JOIN (select * from lb_PRPLCARCOMPONENTINFO where
                     THIRDFACTORYFLAG != 1 ) B
                on A.LOSSAPPROVALID = B.LOSSAPPROVALID )aa
     inner join ( select  CATE_NAME,DD_CATE_ID   from  A_CATEGORY where  CATE_LEVEL=2) f on aa.MODEL_BRAND=f.CATE_NAME
     where  COMMONNAME is not null


工时取数：SELECT
r.id,
A.LOSSAPPROVALID 定损单ID,
A.LOSSAPPROVALCOMCODE 定损员归属机构,
A.VERIFYFINALDATE 最终核损通过时间,
A.VEHSERINAME 定损车系名称,
w.is4s ,
F.REPAIRTYPE 修理类型,
r.compname 工时项,
C.REGISTNO 报案号,
r.sumveriloss 工时价

FROM
PRPLCARLOSSAPPROVAL A
inner join PRPLCLAIMLOSSITEMS C
on C.ITEMID=A.ITEMID
inner join PRPLCARREPAIRFEE F
on F.ITEMID=A.ITEMID
inner join PRPLCARREPAIRFEE R
ON R.LOSSAPPROVALID=A.LOSSAPPROVALID
inner join  PRPLREPAIRCHANNEL w
on a.ITEMID=w.ITEMID

WHERE
A.VERIFYFLAG in ('1','3') and A.VERIPFLAG in ('1','3','N')
AND CASE WHEN A.VERIPFINALDATE IS NULL OR A.VERIFYFINALDATE >= A.VERIPFINALDATE
THEN A.VERIFYFINALDATE ELSE A.VERIPFINALDATE END
BETWEEN
to_date('2020-1-01 00:00:00','YYYY-MM-DD HH24:MI:SS')
and
to_date('2020-5-31 23:59:59','YYYY-MM-DD HH24:MI:SS')