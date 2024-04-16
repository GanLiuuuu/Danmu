
package io.sustc.service.impl;
import com.github.benmanes.caffeine.cache.Cache;
import io.sustc.dto.*;
import io.sustc.service.DatabaseService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.Cacheable;

import org.springframework.cache.annotation.EnableCaching;
import org.springframework.stereotype.Service;


import javax.sql.DataSource;
import java.sql.*;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Slf4j
public class UserServiceImpl implements UserService {
    @Autowired
    private DataSource dataSource;

    @Autowired
    Cache<String, Object> caffeineCache;

    @Override
    public long register(RegisterUserReq req) {
        String sql = "INSERT INTO member (password, qq, wechat, name,sex,birthday,sign,mid) VALUES (?,?,?,?,?,?,?,?);";
        if(!checkReg(req)){
            return -1;
        }
        long mid = getIncreseId();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,req.getPassword());
            stmt.setString(2, req.getQq());
            stmt.setString(3, req.getWechat());
            stmt.setString(4, req.getName());
            if(req.getSex() == RegisterUserReq.Gender.FEMALE){
                stmt.setInt(5,2);
            }else if(req.getSex() == RegisterUserReq.Gender.MALE){
                stmt.setInt(5,1);
            }else{
                stmt.setInt(5,0);
            }
            stmt.setString(6, req.getBirthday());
            stmt.setString(7, req.getSign());
            stmt.setLong(8,mid);
            stmt.executeUpdate();






        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return mid;
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        long operator = checkAuth(auth);
        if(operator == 0){
            return false;
        }
        if(!checkMid(mid)){
            return false;
        }
        if(mid!=operator&&!checkSuperUser(operator)){
            return false;
        }
        if(mid!=operator && checkSuperUser(mid)){
            return false;
        }
        if(checkDel(auth,mid)==0){
            return false;
        }
        long midQuery = mid;
        String sql = "delete from member where mid = ?;";
        try (Connection conn = dataSource.getConnection();
              PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
//        Thread t1 = new Thread(() -> deleteFollow(mid));
//        Thread t2 = new Thread(() -> deleteVideo0(mid));
//        Thread t3 = new Thread(() -> deleteDan(mid));
//        Thread t4 = new Thread(()->deleteFollow1(mid));
//        t2.start();;
//        t1.start();
//        t3.start();
//        t4.start();
//        try {
//            t1.join();
//            t2.join();
//            t3.join();
//            t4.join();
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

        return true;
    }
    public void deleteFollow(long mid){

        String sql = "delete from follow where  member_id = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,mid);

            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
    public void deleteFollow1(long mid){

        String sql = "delete from follow where followed_id = ? ;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,mid);

            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void deleteVideo0(long midQuery){

        String sql = "delete from likeVideo where mid = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "delete from coin where mid = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "delete from reviewvideo where mid = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "delete from favorite where mid = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        sql = "delete from viewvideo where mid = ? ;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }
    public void deleteDan(long midQuery){

        String sql = "delete from likeDanmu where id in(select id from danmu where danmu.mid = ?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "delete from likeDanmu where mid = ? ;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "delete from danmu where mid = ? ;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }
    public void deleteVideo(long midQuery){

        String sql = "delete from likevideo where bv in(select bv from post where post.owner = ?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "delete from viewvideo where bv in(select bv from post where post.owner = ?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "delete from favorite where bv in(select bv from post where post.owner = ?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "delete from coin where bv in(select bv from post where post.owner = ?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "delete from reviewvideo where bv in(select bv from post where post.owner = ?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "delete from video where bv in(select bv from post where post.owner = ?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);

            stmt.executeUpdate();


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        sql = "delete from post where owner = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,midQuery);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }



    }


    @Override
    public boolean follow(AuthInfo auth, long followeeMid) {
        long midQuery = checkAuth(auth);
        if(midQuery ==0 ){
            return false;
        }
        if(!checkMid(followeeMid)){
            return false;
        }
        if(midQuery == followeeMid){
            return false;
        }
        if(hasFollowed(midQuery,followeeMid)){
            String sql = "delete from follow where member_id = ? and followed_id = ?;";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1,midQuery);
                stmt.setLong(2,followeeMid);
                stmt.executeUpdate();


                stmt.close();
                conn.close();
                return false;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }else{
            String sql = "insert into follow(member_id,followed_id) values (?,?);";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1,midQuery);
                stmt.setLong(2,followeeMid);
                stmt.executeUpdate();


            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return true;

    }

//    @Cacheable(value = "user")
    @Override
    @Cacheable(value = "userInfo",key = "#mid")
    public UserInfoResp getUserInfo(long mid) {
        if(!checkMid(mid)){
            return null;
        }
        int coin = getCoin(mid);
        ArrayList<Long> following  = new ArrayList<>();
        ArrayList<Long> follower= new ArrayList<>();
        ArrayList<String> watched= new ArrayList<>();
        ArrayList<String> liked = new ArrayList<>();
        ArrayList<String> collected = new ArrayList<>();
        ArrayList<String> posted = new ArrayList<>();

        Thread t1 = new Thread(()->getFollowing(mid,following));
        Thread t2 = new Thread(() -> getFollower(mid,follower));
        Thread t3 = new Thread(() -> getWatched(mid,watched));
        Thread t4 = new Thread(() -> getLiked(mid,liked));
        Thread t5 = new Thread(()->getCollected(mid,collected));
        Thread t6 = new Thread(() -> getPost(mid,posted));
        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
        t6.start();
        try{
            t1.join();
            t2.join();
            t3.join();
            t4.join();
            t5.join();
            t6.join();

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        long[]a   = new long[following.size()];
        for (int i = 0; i < following.size(); i++) {
            a[i] = following.get(i);
        }
        long[]b   = new long[follower.size()];
        for (int i = 0; i < follower.size(); i++) {
            b[i] = follower.get(i);
        }
        String[] c = new String[watched.size()];
        for (int i = 0; i < watched.size(); i++) {
            c[i] = watched.get(i);
        }
        String[] d = new String[liked.size()];
        for (int i = 0; i < liked.size(); i++) {
            d[i] = liked.get(i);
        }
        String[] e = new String[collected.size()];
        for (int i = 0; i < collected.size(); i++) {
            e[i] = collected.get(i);
        }
        String[]f = new String[posted.size()];
        for (int i = 0; i < f.length; i++) {
            f[i] = posted.get(i);
        }
        UserInfoResp userInfoResp = new UserInfoResp(mid,coin, a,b,c,d,e,f);
        return userInfoResp;

    }
    public boolean checkReg(RegisterUserReq req){
//        if(req.getBirthday()!=null && !req.getBirthday().isEmpty()){
//            String pattern = "XX月YY日";
//            String p2 = "X月YY日";
//            String  p3 = "XX月Y日";
//            String p4 = "X月Y日";
//            String parseIn = req.getBirthday();
//            boolean a = true;
//            boolean b = true;
//            boolean c = true;
//            boolean d = true;
//            try {
//                DateTimeFormatter f = DateTimeFormatter.ofPattern(pattern);
//                LocalDate l = LocalDate.parse(parseIn,f);
//            }catch (DateTimeException e){
//                a = false;
//            }
//            try {
//                DateTimeFormatter f = DateTimeFormatter.ofPattern(p2);
//                LocalDate l = LocalDate.parse(parseIn,f);
//            }catch (DateTimeException e){
//                b = false;
//            }
//            try {
//                DateTimeFormatter f = DateTimeFormatter.ofPattern(p3);
//                LocalDate l = LocalDate.parse(parseIn,f);
//            }catch (DateTimeException e){
//                c = false;
//            }
//            try {
//                DateTimeFormatter f = DateTimeFormatter.ofPattern(p4);
//                LocalDate l = LocalDate.parse(parseIn,f);
//            }catch (DateTimeException e){
//                d = false;
//            }
//            if(!a&&!b&&!c&&!d){
//                return false;
//            }
//        }
        if(req.getBirthday()!=null && !req.getBirthday().isEmpty()){

            String regex = "(\\d+)月(\\d+)日";

            Pattern pattern = Pattern.compile(regex);


            Matcher matcher = pattern.matcher(req.getBirthday());


            if (matcher.matches()) {
                int m = Integer.valueOf(matcher.group(1));
                int d = Integer.valueOf(matcher.group(2));


                if(m<1 || m > 12){
                    return false;
                }
                if(d<1 || d > 31){
                    return false;
                }
                if(d==30 && m ==2){
                    return false;
                }
                if(d==31){
                    if(m==2||m==4||m==6||m==9||m==11){
                        return false;
                    }
                }
            } else {
                return false;
            }


        }
        if(req.getPassword() == null || req.getName() == null ||req.getSex()==null){
            return false;
        }
        if(req.getQq()!=null && req.getWechat()!=null){
            String sql = "SELECT mid from member where name = ? or member.qq = ? or member.wechat = ?";

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, req.getName());
                stmt.setString(2, req.getQq());
                stmt.setString(3, req.getWechat());


                ResultSet rs = stmt.executeQuery();

                if(rs.next()){
                    rs.close();
                    stmt.close();
                    conn.close();
                    return false;
                }
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        else if(req.getQq() != null){
            String sql = "SELECT mid from member where member.name = ? or member.qq = ? ";

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, req.getName());
                stmt.setString(2, req.getQq());



                ResultSet rs = stmt.executeQuery();

                if(rs.next()){
                    rs.close();
                    stmt.close();
                    conn.close();
                    return false;
                }
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }else if(req.getWechat()!=null){
            String sql = "SELECT mid from member where member.name = ? or  member.wechat = ?";

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, req.getName());

                stmt.setString(2, req.getWechat());


                ResultSet rs = stmt.executeQuery();

                if(rs.next()){
                    rs.close();
                    stmt.close();
                    conn.close();
                    return false;
                }
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        else{
            String sql = "SELECT mid from member where member.name = ? ";

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, req.getName());



                ResultSet rs = stmt.executeQuery();

                if(rs.next()){
                    rs.close();
                    stmt.close();
                    conn.close();
                    return false;
                }
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        return true;


    }
    public long checkDel(AuthInfo auth, long mid){
        String sql;
        if(auth.getQq()!=null && auth.getWechat()!=null){
            sql = "SELECT * from member where wechat = ? or qq = ? ";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, auth.getWechat());
                stmt.setString(2, auth.getQq());
                ResultSet rs = stmt.executeQuery();
                rs.next();
                if(rs.next()){
                    rs.close();
                    stmt.close();
                    conn.close();
                    return 0;
                }
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        int identity;
        long midQuery;
        sql = "SELECT * from member where mid = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            ResultSet rs = stmt.executeQuery();
            if(!rs.next()){
                rs.close();
                if(auth.getQq()==null && auth.getWechat()==null){
                    stmt.close();
                    conn.close();
                    return 0;
                } else if (auth.getQq() == null) {
                    sql = "SELECT * from member where wechat = ?";
                    try (Connection conn1 = dataSource.getConnection();
                         PreparedStatement stmt1 = conn1.prepareStatement(sql)) {
                        stmt1.setString(1, auth.getWechat());
                        ResultSet rs1 = stmt1.executeQuery();

                        if(!rs1.next()){
                            rs1.close();
                            stmt1.close();
                            conn1.close();
                            stmt.close();
                            conn.close();
                            return 0;
                        }else{
                            midQuery = rs1.getLong(1);
                            identity = rs1.getInt(10);
                        }
                        rs1.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    sql = "SELECT * from member where qq = ? ";
                    try (Connection conn2 = dataSource.getConnection();
                         PreparedStatement stmt2 = conn2.prepareStatement(sql)) {
                        stmt2.setString(1, auth.getQq());
                        ResultSet rs2 = stmt2.executeQuery();

                        if(!rs2.next()){
                            rs2.close();
                            stmt2.close();
                            conn2.close();
                            stmt.close();
                            conn.close();
                            return 0;
                        }else{
                            midQuery = rs2.getLong(1);
                            identity = rs2.getInt(10);
                        }

                        rs2.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }else{
                if(rs.getString(11).equals(auth.getPassword())){
                    identity = rs.getInt(10);
                    midQuery = rs.getLong(1);
                    rs.close();
                }
                else{
                    rs.close();
                    stmt.close();
                    conn.close();
                    return 0;
                }

            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if(identity==0 && midQuery!=mid){
            return 0;
        }
        return midQuery;
    }
    public boolean hasFollowed(long midQuery,long followeeMid){
        String sql = "select followed_id from follow where followed_id = ? and member_id = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(2,midQuery);
            stmt.setLong(1,followeeMid);
            stmt.executeQuery();
            ResultSet rs =stmt.getResultSet();
            if(!rs.next()){
                rs.close();
                stmt.close();
                conn.close();
                return false;
            }
            rs.close();
            stmt.close();
            conn.close();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean checkMid(long mid){
        String sql = "SELECT * from member where member.mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);



            ResultSet rs = stmt.executeQuery();

            if(!rs.next()){
                rs.close();
                return false;
            }else{
                rs.close();
                return true;
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }
    public int getCoin(long mid){
        String sql = "SELECT * from member where member.mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, mid);



            ResultSet rs = stmt.executeQuery();
            rs.next();

            int i= rs.getInt(7);


            rs.close();
            stmt.close();
            conn.close();
            return i;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void getFollowing(long mid,ArrayList<Long>following){
        String sql = "SELECT followed_id from follow where member_id = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFetchSize(1000);
            stmt.setLong(1, mid);
            ResultSet rs = stmt.executeQuery();
            rs.setFetchSize(1000);
            while (rs.next()){
                following.add(rs.getLong(1));
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void getFollower(long mid,ArrayList<Long> follower){
        String sql = "SELECT member_id from follow where followed_id = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFetchSize(1000);
            stmt.setLong(1, mid);



            ResultSet rs = stmt.executeQuery();
            rs.setFetchSize(1000);
            while (rs.next()){
                follower.add(rs.getLong(1));
            }

            rs.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void getWatched(long mid,ArrayList<String> w){
        String sql = "SELECT * from viewVideo where mid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFetchSize(1000);
            stmt.setLong(1, mid);



            ResultSet rs = stmt.executeQuery();
            rs.setFetchSize(1000);
            while (rs.next()){
                w.add(rs.getString(2));
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void getLiked(long mid,ArrayList<String> l){
        String sql = "SELECT * from likeVideo where mid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFetchSize(1000);
            stmt.setLong(1, mid);



            ResultSet rs = stmt.executeQuery();
            rs.setFetchSize(1000);
            while (rs.next()){
                l.add(rs.getString(2));
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void getCollected(long mid,ArrayList<String> c){
        String sql = "SELECT * from favorite where mid = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFetchSize(1000);
            stmt.setLong(1, mid);



            ResultSet rs = stmt.executeQuery();
            rs.setFetchSize(1000);
            while (rs.next()){
                c.add(rs.getString(2));
            }

            rs.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void getPost(long mid,ArrayList<String> p){
        String sql = "SELECT * from post where owner = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFetchSize(1000);
            stmt.setLong(1, mid);



            ResultSet rs = stmt.executeQuery();
            rs.setFetchSize(1000);
            while (rs.next()){
                p.add(rs.getString(2));
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public long getIncreseId(){
        String sql = "select max(mid) from member;";
        long res;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {



            ResultSet rs = stmt.executeQuery();

            rs.next();
            res = rs.getLong(1) + (long)1;

            rs.close();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return res;
    }
    public long checkAuth(AuthInfo auth){
        String sql;
        if(auth.getQq()!=null && auth.getWechat()!=null){
            sql = "SELECT mid from member where wechat = ? or qq = ? ";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, auth.getWechat());
                stmt.setString(2, auth.getQq());
                ResultSet rs = stmt.executeQuery();
                rs.next();
                if(rs.next()){
                    rs.close();
                    stmt.close();
                    conn.close();
                    return 0;
                }
                rs.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        int identity;
        long midQuery;
        sql = "SELECT mid,identity,password from member where mid = ? ";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1, auth.getMid());
            ResultSet rs = stmt.executeQuery();
            if(!rs.next()){
                rs.close();
                if(auth.getQq()==null && auth.getWechat()==null){
                    stmt.close();
                    conn.close();
                    return 0;
                } else if (auth.getQq() == null) {
                    sql = "SELECT mid,identity from member where wechat = ?";
                    try (Connection conn1 = dataSource.getConnection();
                         PreparedStatement stmt1 = conn1.prepareStatement(sql)) {
                        stmt1.setString(1, auth.getWechat());
                        ResultSet rs1 = stmt1.executeQuery();

                        if(!rs1.next()){
                            rs1.close();
                            stmt1.close();
                            conn1.close();
                            stmt.close();
                            conn.close();
                            return 0;
                        }else{
                            midQuery = rs1.getLong(1);
                            identity = rs1.getInt(2);
                        }
                        rs1.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    sql = "SELECT mid,identity from member where qq = ? ";
                    try (Connection conn2 = dataSource.getConnection();
                         PreparedStatement stmt2 = conn2.prepareStatement(sql)) {
                        stmt2.setString(1, auth.getQq());
                        ResultSet rs2 = stmt2.executeQuery();

                        if(!rs2.next()){
                            rs2.close();
                            stmt2.close();
                            conn2.close();
                            stmt.close();
                            conn.close();
                            return 0;
                        }else{
                            midQuery = rs2.getLong(1);
                            identity = rs2.getInt(2);
                        }

                        rs2.close();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }else{
                if(rs.getString(3).equals(auth.getPassword())){
                    identity = rs.getInt(2);
                    midQuery = rs.getLong(1);
                    rs.close();
                }
                else{
                    rs.close();
                    stmt.close();
                    conn.close();
                    return 0;
                }

            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return midQuery;
    }
    public boolean checkSuperUser(long mid){
        String sql = "SELECT * from member where member.mid = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setLong(1, mid);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            if(rs.getInt(10)==1){
                rs.close();
                stmt.close();
                conn.close();
                return true;

            }
            rs.close();
            stmt.close();
            conn.close();
            return false;
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }



}
