// ConsoleCallbackHandler.java
package org.kafka.demo.other.jaas;

import javax.security.auth.callback.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ConsoleCallbackHandler implements CallbackHandler {
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback) {
                NameCallback nc = (NameCallback) callback;
                System.out.print(nc.getPrompt());
                nc.setName(new BufferedReader(new InputStreamReader(System.in)).readLine());
            } else if (callback instanceof PasswordCallback) {
                PasswordCallback pc = (PasswordCallback) callback;
                System.out.print(pc.getPrompt());
                pc.setPassword(new BufferedReader(new InputStreamReader(System.in)).readLine().toCharArray());
            } else {
                throw new UnsupportedCallbackException(callback, "不支持的回调类型");
            }
        }
    }
}