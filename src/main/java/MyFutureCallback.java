import com.google.common.util.concurrent.FutureCallback;

import com.datastax.driver.core.ResultSet;

class MyFutureCallback implements FutureCallback<ResultSet> {

  private int successCount = 0;
  private int errorCount = 0;

  MyFutureCallback() {

  }

  public int getSuccessCount() {
    return successCount;
  }

  public int getErrorCount() {
    return errorCount;
  }

  @Override
  public void onSuccess(ResultSet result) {
    this.successCount++;

  }

  @Override
  public void onFailure(Throwable t) {
    this.errorCount++;

  }
}
