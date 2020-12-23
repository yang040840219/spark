package org.apache.spark.sql.test;

import com.google.common.util.concurrent.RateLimiter;
import org.junit.Test;
import org.mockito.Mockito;

public class MockTest {

  public class LoginService {
    public String service() {
      System.out.println("service .....");
      return "realtime service" ;
    }
  }


  @Test
  public void simple(){

    LoginService spyLoginService = Mockito.spy(new LoginService()) ;

    Mockito.doReturn("spy realtime service", new Object[]{}).when(spyLoginService).service();

    System.out.println(spyLoginService.service());

  }

  @Test
  public void limit(){

    RateLimiter rateLimiter = RateLimiter.create(1) ;
    for(int i=0; i<100; i++){
      rateLimiter.acquire();
      System.out.println(i);
    }
  }
}
