package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.DanmuService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
    @Autowired
    private DataSource dataSource;
    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        long mid = checkAuth(auth);
        if(mid == 0){
            return -1;
        }
        if(!checkBV(bv)){
            return -1;
        }
        if(content==null || content.isEmpty()){
            return -1;
        }
        if(!checkVideo(bv)){
            return -1;
        }
        if(!checkWatched(auth,bv)){
            return -1;
        }
        if(outOfDuration(time,bv)){
            return -1;
        }
        String sql =
                "INSERT INTO danmu (bv, mid, displayTime, content, postTime, id) VALUES (?, ?, ?, ?, ?, ?)";


        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);
            stmt.setLong(2,mid);
            stmt.setFloat(3, time);
            stmt.setString(4, content);
            stmt.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now()));
            long l = generateId();
            stmt.setLong(6,l);
            stmt.executeUpdate();
            stmt.close();
            conn.close();
            return l;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
        if(!checkBV(bv)){
            return null;
        }
        if(timeEnd==0 || timeStart==0||timeEnd-timeStart<=0 || timeStart > getDuration(bv) || timeEnd > getDuration(bv)){
            return null;
        }
        if(!checkVideo(bv)){
            return null;
        }
        List<Long> res = new ArrayList<>();
        List<String> tmp = new ArrayList<>();
        String sql = "select * from danmu where displayTime >= ? and displayTime <= ? and bv = ? order by displayTime;";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setFloat(1,timeStart);
            stmt.setFloat(2,timeEnd);
            stmt.setString(3,bv);

            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();

            while(rs.next()){
                if(filter){
                    if(!tmp.contains(rs.getString(5))){
                        tmp.add(rs.getString(5));
                        res.add(rs.getLong(1));
                    }
                }else{
                    res.add(rs.getLong(1));
                }
            }
            rs.close();
            stmt.close();
            conn.close();
            return res;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        long mid = checkAuth(auth);
        if(!checkDanmu(id)){
        return false;}
        if(mid==0){
            return false;
        }
        if(!checkWatched(auth,getBV(id))){
            return false;
        }
        if(hasLiked(mid,id)){
            String sql = "delete from likeDanmu where id = ? and mid = ?;";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1,id);
                stmt.setLong(2,mid);
                stmt.executeUpdate(sql);
                stmt.close();
                conn.close();

                return false;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }

        }else{
            String sql = "INSERT INTO likeDanmu (id,mid) VALUES (?,?);";

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1,id);
                stmt.setLong(2,mid);
                stmt.executeUpdate();

                stmt.close();
                conn.close();

                return true;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }



    }
    public float getDuration(String bv){
        String sql = "select duration from video where bv = ?;";
        float res;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            res = rs.getFloat(1);
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return res;
    }
    public long checkAuth(AuthInfo auth){
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

        return midQuery;
    }
    public boolean checkBV(String bv){
        String sql = "select * from video where bv = ?;";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);


            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
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
    public boolean checkVideo(String bv){
        String sql = "select * from video where bv = ?;";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);


            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            rs.next();
            Timestamp t = rs.getTimestamp(4);
            rs.close();
            if(t.before(Timestamp.valueOf(LocalDateTime.now()))){
                rs.close();
                stmt.close();
                conn.close();
                return true;
            }
            rs.close();
            stmt.close();
            conn.close();
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean checkWatched(AuthInfo auth,String bv){
        long mid = checkAuth(auth);
        String sql = "select * from viewvideo where mid = ? and bv = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,mid);
            stmt.setString(2,bv);
            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            if (!rs.next()) {
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
    public boolean checkDanmu(long id){
        String sql = "select * from danmu where id = ?;";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,id);


            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            if (!rs.next()) {
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
    public boolean hasLiked(long mid, long id){
        String sql = "select mid from likeDanmu where id = ? and mid = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,id);
            stmt.setLong(2,mid);
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
    public boolean outOfDuration(float time,String bv){
        String sql = "select duration from video where bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);

            stmt.executeQuery();
            ResultSet rs =stmt.getResultSet();
            rs.next();
            if(rs.getFloat(1)<time){
                rs.close();
                stmt.close();
                conn.close();
                return true;
            }
            rs.close();
            stmt.close();
            conn.close();
            return false;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public synchronized long generateId(){
        long timeNow = System.currentTimeMillis();
        Random random = new Random();
        long sequence = random.nextLong();
        long id = Math.abs((timeNow) | (sequence));
        if(duplicate(id)){
            return generateId();
        }
        return id;
    }
    public boolean duplicate(long id){
        String sql = "select * from danmu where id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,id);
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
    public String getBV(long id){
        String sql = "select bv from danmu where id = ?";
        String res;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,id);

            stmt.executeQuery();
            ResultSet rs =stmt.getResultSet();
            rs.next();
            res = rs.getString(1);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return res;
    }


}
