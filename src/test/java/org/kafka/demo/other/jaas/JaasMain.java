package org.kafka.demo.other.jaas;

import javax.security.auth.*;
import javax.security.auth.login.*;
import java.security.Principal;
import java.security.PrivilegedAction;

public class JaasMain {
    public static void main(String[] args) {
        try {
            // 指定配置文件路径
            System.setProperty("java.security.auth.login.config", 
                    JaasMain.class.getResource("/jaas.config").getPath());
            
            // 创建 LoginContext，指定配置名
            LoginContext lc = new LoginContext("SampleLogin", new ConsoleCallbackHandler());
            
            // 执行登录
            System.out.println("请输入认证信息：");
            lc.login();
            System.out.println("认证成功！");
            
            // 获取已认证的 Subject
            Subject subject = lc.getSubject();
            System.out.println("Subject: " + subject);
            
            // 执行特权操作
            Subject.doAsPrivileged(subject, 
                (PrivilegedAction<Void>) () -> {
                    System.out.println("\n执行特权操作：");
                    checkAccess("admin");  // 检查是否有 admin 角色
                    checkAccess("user");   // 检查是否有 user 角色
                    checkAccess("guest");  // 检查是否有 guest 角色
                    return null;
                }, 
                null);
            
            // 登出
            lc.logout();
            System.out.println("\n已登出");
        } catch (LoginException le) {
            System.err.println("登录失败: " + le.getMessage());
        } catch (SecurityException se) {
            System.err.println("安全错误: " + se.getMessage());
        }
    }
    
    private static void checkAccess(String role) {
        Subject subject = Subject.getSubject(java.security.AccessController.getContext());
        boolean hasRole = false;
        
        if (subject != null) {
            for (Principal principal : subject.getPrincipals()) {
                if (principal instanceof RolePrincipal && principal.getName().equals(role)) {
                    hasRole = true;
                    break;
                }
            }
        }
        
        System.out.printf("用户是否有 %s 角色: %s%n", role, hasRole);
    }
}