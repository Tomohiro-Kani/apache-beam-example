import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ApacheBeamExample {
  private static final Logger LOG = LoggerFactory.getLogger(ApacheBeamExample.class);

  public static void main(String[] args) {
    List<String> goods = getInitialDataGoods();
    List<KV<String,String>> prices = getInitialDataPriceGoodsKV();
    List<KV<String,String>> goodsSizeKV = getInitialDataGoodsSizeKV();
    List<KV<String,String>> goodsSalesKV = getInitialDataGoodsSalesKV();
    List<Integer> pantsSales = getInitialDataPantsSales();

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    /////////////////////////
    // Goods
    /////////////////////////
    PCollection<String> goodsCollection = pipeline.apply(
      "Goods",
      Create.of(goods)
    );

    PCollection<String> goodsNameCollection = goodsCollection.apply(
      "Goods Name extraction",
      ParDo.of(
        new DoFn<String, String>() {  //左がInput、右がOutputの型
          @ProcessElement
          public void processElement(ProcessContext c) {
            List<String> line = Arrays.asList(c.element().split(","));
            c.output(line.get(1));
            System.out.println("Goods Name extraction:" + line.get(1));
          }
        }
      )
    );

    /////////////////////////
    // Price
    /////////////////////////
    PCollection<KV<String,String>> priceCollection = pipeline.apply(
      "Price Grouping",
      Create.of(prices)
    );

    PCollection<String> priceGroupCollection =
      priceCollection
        .apply(
          GroupByKey.create())
      .apply(
        "KV to String",
        ParDo.of(
          new DoFn<KV<String, Iterable<String>>, String>() {
            @ProcessElement
            public void prcessElement(ProcessContext c){
              KV<String,Iterable<String>> elem = c.element();
              String output = "";
              for(String e : elem.getValue() ) {
                if(!StringUtils.isEmpty(output)){
                  output = output + "," + e;
                }else{
                  output = e;
                }
              }
              c.output(elem.getKey() + ":" + output);
              System.out.println("KV<String, Iterable<String>> to String:" + elem.getKey() + ":" + output);
            }
          }
        )
      );

    /////////////////////////
    // Goods & Sales
    /////////////////////////
    PCollection<KV<String,String>> goodsSizeKVCollection = pipeline.apply("GoodsSize KV",Create.of(goodsSizeKV));
    PCollection<KV<String,String>> goodsSalesKVCollection = pipeline.apply("GoodsSales KV",Create.of(goodsSalesKV));

    final TupleTag<String> goodsSizeTag = new TupleTag<>();
    final TupleTag<String> goodsSalesTag = new TupleTag<>();
    PCollection<String> goodsSizeSalesCollection =
      KeyedPCollectionTuple.of(goodsSizeTag,goodsSizeKVCollection)
      .and(goodsSalesTag,goodsSalesKVCollection)
      .apply(CoGroupByKey.create())
      .apply(
        "KV<String, CoGbkResult> to String",
        ParDo.of(
          new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void prcessElement(ProcessContext c){
              KV<String, CoGbkResult> elem = c.element();
              Iterable<String> goodsSize = elem.getValue().getAll(goodsSizeTag);
              Iterable<String> goodsSales = elem.getValue().getAll(goodsSalesTag);
              String output = "";
              for(String size : goodsSize ) {
                if(!StringUtils.isEmpty(output)){
                  output = output + "," + size;
                }else{
                  output = size;
                }
              }
              for(String sales : goodsSales ) {
                if(!StringUtils.isEmpty(output)){
                  output = output + "," + sales;
                }else{
                  output = sales;
                }
              }
              c.output(elem.getKey() + ":" + output);
              System.out.println("KV<String, CoGbkResult> to String:" + elem.getKey() + ":" + output);
            }
          }
        )
      );

    /////////////////////////
    // Pants Sales
    // https://www.codota.com/code/java/methods/org.apache.beam.sdk.transforms.Combine$Globally/withoutDefaults
    /////////////////////////
    PCollection<Integer> pantsSalesCollection = pipeline.apply(
      "Goods",
      Create.of(pantsSales)
    );

    PCollection<String> pantsSalesSumCollection = pantsSalesCollection.apply(
      Sum.integersGlobally())
      .apply(
        "PantsSalesSum convert string",
        ParDo.of(
          new DoFn<Integer, String>() {  //左がInput、右がOutputの型
            @ProcessElement
            public void processElement(ProcessContext c) {
              c.output(c.element().toString());
              System.out.println("PantsSalesSum convert string:" + c.element().toString());
            }
          }
        )
      );
//    PCollection<Integer> sum = pantsSalesCollection.apply(
//      Combine.globally(new Sum.ofIntegers()));


    goodsNameCollection.apply(TextIO.write().to("./src/main/resources/goodsName.txt").withoutSharding());
    priceGroupCollection.apply(TextIO.write().to("./src/main/resources/priceGroup.txt").withoutSharding());
    goodsSizeSalesCollection.apply(TextIO.write().to("./src/main/resources/goodsSizeSales.txt").withoutSharding());
    goodsCollection.apply(TextIO.write().to("./src/main/resources/goodsSales.txt").withoutSharding());
    pantsSalesSumCollection.apply(TextIO.write().to("./src/main/resources/pantsSalesSum.txt").withoutSharding());
    pipeline.run().waitUntilFinish();
  }

  public static List<String> getInitialDataGoods() {
    List<String> goods = new ArrayList<>();
    goods.add("1,Tシャツ");  //Id,Name
    goods.add("2,ポロシャツ");
    goods.add("3,パーカー");
    goods.add("4,ジャージ");
    goods.add("5,ブルゾン");
    goods.add("6,ダウンジャケット");
    goods.add("7,ピーコート");
    goods.add("8,スタジャン");
    goods.add("9,デニムパンツ");
    return goods;
  }

  public static List<String> getInitialDataSales() {
    List<String> sales = new ArrayList<>();
    sales.add("1,1000");  //Id,Sales
    sales.add("2,2000");
    sales.add("3,3000");
    sales.add("4,4000");
    sales.add("5,5000");
    sales.add("6,6000");
    sales.add("7,7000");
    sales.add("8,8000");
    sales.add("9,9000");
    return sales;
  }

  public static List<KV<String,String>> getInitialDataGoodsSizeKV() {
    List<KV<String,String>> goods = new ArrayList<>();
    goods.add(KV.of("Tシャツ","S"));
    goods.add(KV.of("Tシャツ","M"));
    goods.add(KV.of("Tシャツ","L"));
    goods.add(KV.of("ジャージ","S"));
    goods.add(KV.of("ジャージ","L"));
    goods.add(KV.of("ダウンジャケット","XL"));
    return goods;
  }

  public static List<KV<String,String>> getInitialDataGoodsSalesKV() {
    List<KV<String,String>> sales = new ArrayList<>();
    sales.add(KV.of("Tシャツ","10000"));
    sales.add(KV.of("Tシャツ","20000"));
    sales.add(KV.of("ジャージ","50000"));
    sales.add(KV.of("デニムパンツ","4000"));
    sales.add(KV.of("デニムパンツ","30000"));
    return sales;
  }

  public static List<KV<String,String>> getInitialDataPriceGoodsKV(){
    List<KV<String,String>> price = new ArrayList<>();
    price.add(KV.of("1000","Tシャツ"));
    price.add(KV.of("1000","タオル"));
    price.add(KV.of("1000","靴下"));
    price.add(KV.of("2000","デニムパンツ"));
    price.add(KV.of("2000","ビジネスシャツ"));
    price.add(KV.of("3000","ジャケット"));
    return price;
  }

  public static List<Integer> getInitialDataPantsSales(){
    List<Integer> pants = new ArrayList<>();
    pants.add(4000);
    pants.add(30000);
    pants.add(10000);
    return pants;
  }
}
