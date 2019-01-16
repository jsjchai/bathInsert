package bathInsert;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import model.DB;
import model.User;

public class BathInsert {

    
    private static List<User> getData(){
        List<User> userList=new ArrayList<>();
        for(int i=1;i<900000;i++){
            User user=new User();
            user.setId(i);
            user.setUsername(UUID.randomUUID().toString());
            user.setEmail("test13"+ i +"@test.com");
            userList.add(user);
        }
        return userList;
    }
    /**
     * 每次只插入一条 19308ms
     * @throws SQLException 
     */
    public void bathInsert1(List<User> userList) {
        if(userList!=null&&userList.size()>0){
            Connection conn=DB.getConnection();
            Statement stat=DB.getStatement(conn);
            for(User u:userList){
                String sql="insert into user values("+u.getId()+",'"+u.getUsername()+"','"+u.getEmail()+"')";
                DB.executeUpdate(stat, sql);
            }
            DB.close(stat);
            DB.close(conn);
        }
      
    }
    
    /**
     * 手动提交 19140ms
     * @param userList
     * @throws SQLException
     */
    public void bathInsert2(List<User> userList) throws SQLException{
        if(userList!=null&&userList.size()>0){
            Connection conn=DB.getConnection();
            conn.setAutoCommit(false);
            Statement stat=DB.getStatement(conn);
            for(User u:userList){
                String sql="insert into user values("+u.getId()+",'"+u.getUsername()+"','"+u.getEmail()+"')";
                DB.executeUpdate(stat, sql);
            }
            conn.commit();
            DB.close(stat);
            DB.close(conn);
        }
      
    }
    
    /**
     * PreparedStatement 19756ms 19148ms 19489ms
     * @param userList
     * @throws SQLException
     */
    public void bathInsert3(List<User> userList) throws SQLException {
        if(userList!=null&&userList.size()>0){
            Connection conn=DB.getConnection();
            conn.setAutoCommit(false);
            String sql="insert into user values(?,?,?)";
            PreparedStatement pre=DB.getPreparedStatement(conn, sql);
            for(User u:userList){
                pre.setLong(1,u.getId());
                pre.setString(2, u.getUsername());
                pre.setString(3, u.getEmail());
                pre.executeUpdate();
            }
            conn.commit();
            DB.close(pre);
            DB.close(conn);
        }
      
    }
    
    /**
     * 批处理   21362ms 22228ms
     * @param userList
     * @throws SQLException
     */
    public void bathInsert4(List<User> userList) throws SQLException{
        if(userList!=null&&userList.size()>0){
            Connection conn=DB.getConnection();
            conn.setAutoCommit(false);
            Statement stat=DB.getStatement(conn);
            for(User u:userList){
                String sql="insert into user values("+u.getId()+",'"+u.getUsername()+"','"+u.getEmail()+"')";
               stat.addBatch(sql);
            }
            stat.executeBatch();
            conn.commit();
            DB.close(stat);
            DB.close(conn);
        }
    }
    
    /**
     * insert into user values(),(),() 手动提交  1764ms 1678ms 1662ms
     * @param userList
     * @throws SQLException
     */
    public void bathInsert5(List<User> userList) throws SQLException{
        if(userList!=null&&userList.size()>0){
            Connection conn=DB.getConnection();
            conn.setAutoCommit(false);
            Statement stat=DB.getStatement(conn);
            StringBuffer buf=new StringBuffer();
            buf.append("insert into user values");
            String str = null;
            for(User u:userList){
                buf.append("(").append(str).append(",'").append(u.getUsername()).append("','").append(u.getEmail()).append("'),");
            }
            stat.executeUpdate(buf.substring(0, buf.length()-1));
            conn.commit();
            DB.close(stat);
            DB.close(conn);
        }
    }
    
   
    public static void main(String[] args) throws SQLException {
        BathInsert b=new BathInsert();
        long start=System.currentTimeMillis();
        b.bathInsert5(BathInsert.getData());
        long end=System.currentTimeMillis();
        System.out.println("插入200000条数据耗时"+(end-start)+"ms");
        
    }
}
