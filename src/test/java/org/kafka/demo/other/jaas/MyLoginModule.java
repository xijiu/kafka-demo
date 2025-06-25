// MyLoginModule.java
package org.kafka.demo.other.jaas;

import javax.security.auth.*;
import javax.security.auth.callback.*;
import javax.security.auth.login.*;
import javax.security.auth.spi.*;
import java.io.IOException;
import java.security.Principal;
import java.util.Map;

public class MyLoginModule implements LoginModule {
    private Subject subject;
    private CallbackHandler callbackHandler;
    private Map<String, ?> options;
    
    // 认证状态
    private boolean succeeded = false;
    private boolean commitSucceeded = false;
    
    // 认证成功后的用户信息
    private String username;
    private Principal userPrincipal;
    
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler,
                           Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.options = options;
    }
    
    @Override
    public boolean login() throws LoginException {
        if (callbackHandler == null) {
            throw new LoginException("没有提供 CallbackHandler");
        }
        
        Callback[] callbacks = new Callback[2];
        callbacks[0] = new NameCallback("用户名: ");
        callbacks[1] = new PasswordCallback("密码: ", false);
        
        try {
            callbackHandler.handle(callbacks);
            String name = ((NameCallback) callbacks[0]).getName();
            String password = new String(((PasswordCallback) callbacks[1]).getPassword());
            
            // 简单验证（实际应用中应使用安全存储和加密）
            if ("admin".equals(name) && "password123".equals(password)) {
                username = name;
                succeeded = true;
                return true;
            } else {
                throw new FailedLoginException("认证失败：用户名或密码错误");
            }
        } catch (IOException | UnsupportedCallbackException e) {
            throw new LoginException("处理回调时出错: " + e.getMessage());
        }
    }
    
    @Override
    public boolean commit() throws LoginException {
        if (!succeeded) {
            return false;
        }
        
        // 创建 Principal 并添加到 Subject
        userPrincipal = new UserPrincipal(username);
        if (!subject.getPrincipals().contains(userPrincipal)) {
            subject.getPrincipals().add(userPrincipal);
        }
        
        // 添加角色（模拟授权）
        subject.getPrincipals().add(new RolePrincipal("admin"));
        subject.getPrincipals().add(new RolePrincipal("user"));
        
        commitSucceeded = true;
        return true;
    }
    
    @Override
    public boolean abort() throws LoginException {
        if (!succeeded) {
            return false;
        } else if (succeeded && !commitSucceeded) {
            // 重置状态
            succeeded = false;
            username = null;
            userPrincipal = null;
        } else {
            // 认证已提交，需要回滚
            logout();
        }
        return true;
    }
    
    @Override
    public boolean logout() throws LoginException {
        subject.getPrincipals().remove(userPrincipal);
        subject.getPrincipals().removeIf(p -> p instanceof RolePrincipal);
        succeeded = false;
        commitSucceeded = false;
        username = null;
        userPrincipal = null;
        return true;
    }
}

// Principal 实现类
class UserPrincipal implements Principal {
    private final String name;
    
    public UserPrincipal(String name) {
        this.name = name;
    }
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public String toString() {
        return "UserPrincipal{" +
                "name='" + name + '\'' +
                '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserPrincipal that = (UserPrincipal) o;
        return name.equals(that.name);
    }
    
    @Override
    public int hashCode() {
        return name.hashCode();
    }
}

class RolePrincipal implements Principal {
    private final String role;
    
    public RolePrincipal(String role) {
        this.role = role;
    }
    
    @Override
    public String getName() {
        return role;
    }
    
    @Override
    public String toString() {
        return "RolePrincipal{" +
                "role='" + role + '\'' +
                '}';
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RolePrincipal that = (RolePrincipal) o;
        return role.equals(that.role);
    }
    
    @Override
    public int hashCode() {
        return role.hashCode();
    }
}