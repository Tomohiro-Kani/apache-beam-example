import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface CustomOptions extends PipelineOptions {
  @Description("Custom command line argument.")
  @Default.String("DEFAULT")
  String getHogeHogeOption();
  void setHogeHogeOption(String hogehogeOption);
}
