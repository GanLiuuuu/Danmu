package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
@Service
@Slf4j
public class RecommenderService implements io.sustc.service.RecommenderService {

    @Autowired
    private DataSource dataSource;
    @Override
    public List<String> recommendNextVideo(String bv) {
        if(!getVideo(bv)){
            return null;
        }
        List<String> res = new ArrayList<>();
        String sql = getSQL();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1,bv);
            stmt.setString(2,bv);
            stmt.setFetchSize(1000);
            stmt.executeQuery();

            ResultSet rs = stmt.getResultSet();
            rs.setFetchSize(1000);
            int i = 0;
            while (rs.next()&&i<5){
                i++;
                res.add(rs.getString(1));
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
    public List<String> generalRecommendations(int pageSize, int pageNum) {
        if(pageNum<=0||pageSize<=0){
            return null;
        }
        String sql = getScore();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1,pageSize);
            stmt.setInt(2,(pageNum-1)*pageSize);
            stmt.setFetchSize(1000);
            stmt.executeQuery();

            ResultSet rs = stmt.getResultSet();
            rs.setFetchSize(1000);
            List<String> res = new ArrayList<>();
            while (rs.next()){
                res.add(rs.getString(1));
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
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {
        long mid = checkAuth(auth);
        if(mid==0){
            return null;
        }
        if(pageNum<=0||pageSize<=0){
            return null;
        }

        String sql = getSQL1();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1,mid);
            stmt.setLong(2,mid);
            stmt.setLong(3,mid);

            stmt.setInt(4,1);
            stmt.setInt(5,0);
            stmt.executeQuery();

            ResultSet pre = stmt.getResultSet();
            if(!pre.next()){
                pre.close();
                stmt.close();
                conn.close();
                return generalRecommendations(pageSize,pageNum);
            }
            stmt.setLong(1,mid);
            stmt.setLong(2,mid);
            stmt.setLong(3,mid);
            stmt.setInt(4,pageSize);
            stmt.setInt(5,(pageNum-1)*pageSize);
            stmt.setFetchSize(1000);
            stmt.executeQuery();

            ResultSet rs = stmt.getResultSet();
            rs.setFetchSize(1000);
            List<String> res = new ArrayList<>();

            while (rs.next()){
                res.add(rs.getString(1));
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
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        long mid = checkAuth(auth);
        if(mid==0){
            return null;
        }
        if(pageNum<=0||pageSize<=0){
            return null;
        }
        String sql = getSQL2();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,mid);
            stmt.setLong(2,mid);
            stmt.setLong(3,mid);
            stmt.setInt(4,pageSize);
            stmt.setInt(5,(pageNum-1)*pageSize);
            stmt.setFetchSize(1000);
            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            rs.setFetchSize(1000);
            List<Long> res = new ArrayList<>();
            while (rs.next()){
                res.add(rs.getLong(1));
            }
            rs.close();
            stmt.close();
            conn.close();
            return res;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean getVideo(String bv){
        String sql = "select bv from video where bv = ?;";
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
    public String getSQL(){
        StringBuilder sb = new StringBuilder();
//        sb.append("Select bv,count(v.mid) as cnt from viewVideo v join (select mid from viewVideo where bv = ?)a on a.mid = v.mid where bv != ?\n" +
//                "  group by bv order by cnt desc,bv;");
        sb.append("Select bv,count(v.mid) as cnt from viewVideo v   where bv != ? and exists(select 1 from viewvideo where viewVideo.mid = v.mid and bv = ?)\n" +
                "                group by bv order by cnt desc,bv;\n");
        return sb.toString();
    }
    //TODO: for query including pages, change the sql like this instead of using for loop
    public String getScore(){
        StringBuilder sb = new StringBuilder();
        sb.append("select bv, sum(score) as s from (select bv,sum(cnt) as score from (\n" +
                "select v.bv, least(1,lv_cnt::numeric/vv_cnt::numeric) as cnt from video v  join (select lvv.bv,count( lvv.mid) as lv_cnt from likeVideo lvv group by lvv.bv)as lv on v.bv = lv.bv  join (select bv,greatest(1,count( mid)) as vv_cnt from viewVideo group by bv) as  vv on vv.bv = v.bv\n" +
                "union all select v.bv,least(1,lv_cnt::numeric/vv_cnt::numeric) as cnt from video v  join (select f.bv,count( f.mid) as lv_cnt from favorite f group by f.bv)as lv on v.bv = lv.bv  join (select bv,greatest(1,count( mid)) as vv_cnt from viewVideo group by bv) as  vv on vv.bv = v.bv\n" +
                "union all select v.bv,least(lv_cnt::numeric/vv_cnt::numeric ,1)as cnt from video v  join (select c.bv,count( c.mid) as lv_cnt from coin c group by c.bv)as lv on v.bv = lv.bv  join (select bv,greatest(1,count( mid)) as vv_cnt from viewVideo group by bv) as  vv on vv.bv = v.bv\n" +
                ") a group by a.bv\n" +
                "union all select v.bv,lv_cnt::numeric/vv_cnt::numeric as score from video v join  (select bv,count( id) as lv_cnt from danmu group by bv )as dan on dan.bv = v.bv join (select bv,greatest(1,count( mid)) as vv_cnt from viewVideo group by bv)as vv on vv.bv = dan.bv\n" +
                "union all select v.bv,(lv_cnt::numeric/v.duration::numeric) as score from video v join (select bv,avg(viewTime) as lv_cnt from viewvideo group by bv) as avg_t on avg_t.bv = v.bv\n" +
                "\n" +
                ")b\n" +
                "\n" +
                "group by bv order by s desc limit ? offset ?;");
        return sb.toString();
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
    public boolean hasFriend(long mid){
        String sql;
            sql = "SELECT member_id from follow where followed_id = ?  and member_id in (select followed_id from follow where member_id = ?)";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setLong(1, mid);
                stmt.setLong(2,mid);
                log.info("SQL: {}", stmt);
                ResultSet rs = stmt.executeQuery();

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
    public String getSQL1(){
        StringBuilder sb = new StringBuilder();
//        sb.append("select b.bv,cnt,publicTime,level\n" +
//                "from\n" +
//                "(select v.bv,coalesce(cnt,0) as cnt,publicTime from\n" +
//                "            (select bv,count( mid) as cnt\n" +
//                "from viewvideo where mid in (SELECT member_id from follow where followed_id = ?  and follow.member_id in (select followed_id from follow where member_id = ?))\n" +
//                "group by bv) a  right join video v on a.bv = v.bv)b\n" +
//                "join post p on p.bv = b.bv join member m on m.mid = p.owner\n" +
//                "where b.bv not in(select bv from viewVideo where mid = ?) and cnt !=0\n" +
//                "group by b.bv,cnt,publicTime,level\n" +
//                "order by cnt desc,level desc,publicTime desc\n" +
//                "limit ? offset ?");
//        sb.append("select b.bv,cnt,publicTime,level\n" +
//                "from\n" +
//                "(select v.bv,coalesce(cnt,0) as cnt,publicTime from\n" +
//                "            (select bv,count( mid) as cnt\n" +
//                "from viewvideo where mid in (SELECT member_id from follow where followed_id = ?  and follow.member_id in (select followed_id from follow where member_id = ?))\n" +
//                "group by bv) a  right join video v on a.bv = v.bv)b\n" +
//                "join post p on p.bv = b.bv join member m on m.mid = p.owner\n" +
//                "where not exists(select bv from viewVideo where mid = ? and viewVideo.bv = b.bv) and cnt !=0\n" +
//                "group by b.bv,cnt,publicTime,level\n" +
//                "order by cnt desc,level desc,publicTime desc\n" +
//                "limit ? offset ?");
        sb.append("select b.bv,cnt,publicTime,level\n" +
                "from\n" +
                "(select v.bv,coalesce(cnt,0) as cnt,publicTime from\n" +
                "            (select bv,count( mid) as cnt\n" +
                "from viewvideo where exists (SELECT 1 from follow f1 where followed_id = ? and f1.member_id = viewVideo.mid  and exists (select 1 from follow where member_id = ? and follow.followed_id = f1.member_id))\n" +
                "group by bv) a  right join video v on a.bv = v.bv)b\n" +
                "join post p on p.bv = b.bv join member m on m.mid = p.owner\n" +
                "where not exists(select bv from viewVideo where mid = ? and viewVideo.bv = b.bv) and cnt !=0\n" +
                "group by b.bv,cnt,publicTime,level\n" +
                "order by cnt desc,level desc,publicTime desc\n" +
                "limit ? offset ?");
        return sb.toString();
    }
    public String getSQL2(){
        StringBuilder sb = new StringBuilder();
//        sb.append("\n" +
//                "select followed_id,cnt,level\n" +
//                "from (select followed_id,count(distinct a.member_id) as cnt\n" +
//                "from\n" +
//                "(select member_id\n" +
//                "from follow where followed_id = ?) a\n" +
//                "join\n" +
//                "(select member_id,followed_id\n" +
//                "from follow) b on a.member_id = b.member_id\n" +
//                "group by followed_id) c join member m on c.followed_id = m.mid\n" +
//                "where followed_id not in (select followed_id from follow where member_id = ?) and followed_id != ?\n" +
//                "order by cnt desc ,level desc ,followed_id limit ? offset ?;");

//        sb.append("select member_id,cnt,level\n" +
//                "                from (select member_id,count(a.followed_id) as cnt\n" +
//                "                from\n" +
//                "                (select followed_id\n" +
//                "                from follow where member_id = ?) a\n" +
//                "                join\n" +
//                "                (select member_id,followed_id\n" +
//                "                from follow) b on a.followed_id = b.followed_id \n" +
//                "                group by member_id) c join member m on c.member_id = m.mid\n" +
//                "                where member_id not in (select followed_id from follow where member_id = ?) and member_id != ? and cnt != 0\n" +
//                "                order by cnt desc ,level desc ,member_id limit ? offset ?;");
        sb.append("select member_id,cnt,level\n" +
                "                from (select member_id,count(a.followed_id) as cnt\n" +
                "                from\n" +
                "                (select followed_id\n" +
                "                from follow where member_id = ?) a\n" +
                "                join\n" +
                "                (select member_id,followed_id\n" +
                "                from follow) b on a.followed_id = b.followed_id \n" +
                "                group by member_id) c join member m on c.member_id = m.mid\n" +
                "                where not exists (select * from follow where follow.followed_id = c.member_id and member_id = ?) and member_id != ? and cnt != 0\n" +
                "                order by cnt desc ,level desc ,member_id limit ? offset ?;");

        return sb.toString();
    }

}
