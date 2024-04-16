package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

@Service
@Slf4j
public class VideoServiceImpl implements VideoService {
    @Autowired
    private DataSource dataSource;
    @Override
    public String postVideo(AuthInfo auth, PostVideoReq req) {
        if (req.getTitle() == null||req.getTitle().isEmpty()) {
            return null;
        }if (req.getDuration()<10){
            return null;
        }if(req.getPublicTime()==null||req.getPublicTime().before(Timestamp.valueOf(LocalDateTime.now()))){
            return null;
        }
        if(checkAuth(auth)==0){
            return null;
        }
        long mid = checkAuth(auth);
        if(!checkVideo(mid,req.getTitle())){
            return null;
        }
        String bv = getRandomString(13);
        String sql = "INSERT INTO post (owner,bv) values (?,?);INSERT INTO video (title,description,duration,publicTime,commitTime,bv) VALUES (?,?,?,?,?,?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(2,bv);
            stmt.setLong(1,mid);
            stmt.setString(3,req.getTitle());
            stmt.setString(4, req.getDescription());
            stmt.setFloat(5, req.getDuration());
            stmt.setTimestamp(6, req.getPublicTime());
            stmt.setTimestamp(7,Timestamp.valueOf(LocalDateTime.now()));
            stmt.setString(8,bv);
            stmt.executeUpdate();

            stmt.close();
            conn.close();
            return bv;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

//        sql = "INSERT INTO post (owner,bv) VALUES (?,?);";
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(sql)) {
//            stmt.setString(2,bv);
//            stmt.setLong(1, mid);
//
//            stmt.executeUpdate();
//
//            stmt.close();
//            conn.close();
//            return bv;
//
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }

    }

    @Override
    public boolean deleteVideo(AuthInfo auth, String bv) {
        long  mid = checkAuth(auth);
        if(mid==0){
            return false;
        }


        long owner = getOwner(bv);
        if(owner == 0){
            return false;
        }


        if(owner!=mid && !checkSuperUser(mid)){
            return false;
        }

        String sql = "delete from post where bv = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);

            stmt.executeUpdate();


            stmt.close();



        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        long mid = checkAuth(auth);
        if(mid==0){
            return false;
        }
        if(!getVideo(bv)){
            return false;
        }
        if(getOwner(bv)!=mid){
            return false;
        }
        if(!checkVideo(bv,req)){
            return false;
        }
        if(req.getPublicTime()==null||req.getPublicTime().before(Timestamp.valueOf(LocalDateTime.now()))){
            return false;
        }
        if (req.getTitle() == null || req.getTitle().isEmpty()) {
            return false;
        }
        if(!duplicatedTitle(mid,bv, req.getTitle())){
            return false;
        }
        String sql = "UPDATE video set title = ? ,description = ?, publicTime = ? where bv = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,req.getTitle());
            stmt.setString(2, req.getDescription());
            stmt.setTimestamp(3, req.getPublicTime());
            stmt.setString(4,bv);
            stmt.executeUpdate();
            if(hasReview(bv)){
                re_review(bv);
                stmt.close();
                conn.close();
                return true;
            }
            
            stmt.close();
            conn.close();
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        long t = System.currentTimeMillis();
        long mid_auth = checkAuth(auth);

        if(mid_auth==0){
            return null;
        }
        if(keywords==null || keywords.isEmpty()){
            return null;
        }
        if(pageSize<=0 || pageNum <=0){
            return null;
        }
        t = System.currentTimeMillis();
        List<String> res = new ArrayList<>();
        String sql = getQuery(keywords);
        String[] wordArr = keywords.split("\\s+");
        if(!checkSuperUser(mid_auth)){
            sql = getQuery1(keywords);
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 0; i < wordArr.length; i++) {
                    for (int j = 0; j < 9; j++) {
                        if(j==2||j==5||j==8){
                            stmt.setLong(1+i*9+j,mid_auth);
                        }else{
                            stmt.setString(1+i*9+j,wordArr[i]);
                        }

                    }
                }

                t = System.currentTimeMillis();
                ResultSet rs = stmt.executeQuery();
                log.info("time of exe:{}",System.currentTimeMillis() - t);
                if(!rs.next()){
                    rs.close();
                    stmt.close();
                    conn.close();
                    return null;
                }
                for (int i = 0; i < (pageNum - 1) * pageSize; i++) {
                    if(!rs.next()){
                        rs.close();
                        stmt.close();
                        conn.close();
                        return null;
                    }
                }
                for (int i = 0; i < pageSize; i++) {
                    res.add(rs.getString(1));
                    if (!rs.next()) {
                        break;
                    }
                }

                rs.close();
                stmt.close();
                conn.close();
                return res;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }else{
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                for (int i = 0; i < wordArr.length; i++) {
                    for (int j = 0; j < 6; j++) {
                        stmt.setString(1+i*6+j,wordArr[i]);
                    }
                }
                t = System.currentTimeMillis();
                ResultSet rs = stmt.executeQuery();
                log.info("second:{}",System.currentTimeMillis() - t);
                if(!rs.next()){
                    rs.close();
                    stmt.close();
                    conn.close();
                    return null;
                }

                for (int i = 0; i < (pageNum - 1) * pageSize; i++) {
                    if(!rs.next()){
                        rs.close();
                        stmt.close();
                        conn.close();
                        return null;
                    }
                }
                for (int i = 0; i < pageSize; i++) {
                    res.add(rs.getString(1));
                    if (!rs.next()) {
                        break;
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


    }

    @Override
    public double getAverageViewRate(String bv) {
        if(!getVideo(bv)){
            return -1;
        }
        if(!checkView(bv)){
            return -1;
        }
        Float duration;
        String sql = "select duration from video where bv = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);


            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            rs.next();
            duration = rs.getFloat(1);
            rs.close();
            stmt.close();
            conn.close();
            return getAVGViewTime(bv)/duration;
//        Float duration;
//        String sql = "select AVG(viewTime)::numeric/v.duration::numeric from viewVideo vv join video v on vv.bv = v.bv and v.bv = ? and vv.bv = ?\n" +
//                "        group by v.duration;";
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(sql)) {
//            stmt.setString(1,bv);
//            stmt.setString(2,bv);
//            stmt.executeQuery();
//            ResultSet rs = stmt.getResultSet();
//            rs.next();
//            float res = rs.getFloat(1);
//            rs.close();
//            stmt.close();
//            conn.close();
//            return res;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }



    }

    @Override
    public Set<Integer> getHotspot(String bv) {
        if(!getVideo(bv)){
            return null;
        }
        if(!getDanmu(bv)){
            return null;
        }
        String sql = "SELECT floor(displayTime / 10) AS index,COUNT(*) AS cnt FROM danmu where bv = ? group by index order by cnt desc ;";
        Set<Integer> res = new HashSet<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);
            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();

            int last_cnt;
            if(!rs.next()){
                rs.close();
                stmt.close();
                conn.close();
                return null;
            }
            res.add(rs.getInt(1));
            last_cnt = rs.getInt(2);
            while (rs.next() &&rs.getInt(2)==last_cnt){
                res.add(rs.getInt(1));
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
    public boolean reviewVideo(AuthInfo auth, String bv) {
        long mid = checkAuth(auth);
        if(mid==0){
            return false;
        }
        if(!getVideo(bv)){
            return false;
        }
        if(!checkSuperUser(mid)){
            return false;
        }
        if(getOwner(bv) == mid){
            return false;
        }
        if(checkReview(bv)){
            return false;
        }

        String sql = "INSERT INTO reviewVideo (mid,bv,reviewTime) VALUES (?,?,?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(2,bv);
            stmt.setLong(1, mid);
            stmt.setTimestamp(3,Timestamp.valueOf(LocalDateTime.now()));

            stmt.executeUpdate();

            stmt.close();
            conn.close();
            return true;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public boolean coinVideo(AuthInfo auth, String bv) {
        long mid = checkAuth(auth);
        if(mid == 0){
            return false;
        }
        if(!getVideo(bv)){
            return false;
        }
        if(!canSearch(mid,bv)){
            return false;
        }
        long coin = getCoin(mid);
        if(coin==-1){
            return false;
        }
        if(hasCoin(bv,mid)){
            return false;
        }
        String sql = "UPDATE member set coin = ? where mid = ?;" +
                "INSERT INTO coin(mid,bv) values (?,?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(4,bv);
            stmt.setLong(3,mid);
            stmt.setLong(1,coin-1);
            stmt.setLong(2,mid);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return true;

    }

    @Override
    public boolean likeVideo(AuthInfo auth, String bv) {
        long mid = checkAuth(auth);
        if(mid == 0){
            return false;
        }
        if(!getVideo(bv)){
            return false;
        }
        if(!canSearch(mid,bv)){
            return false;
        }
        if(hasLike(bv,mid)){
            return unlike(bv,mid);
        }

        return like(bv,mid);

    }

    @Override
    public boolean collectVideo(AuthInfo auth, String bv) {
        long mid = checkAuth(auth);
        if(mid == 0){
            return false;
        }
        if(!getVideo(bv)){
            return false;
        }
        if(!canSearch(mid,bv)){
            return false;
        }if(hasCollect(bv,mid)){
             unCollect(bv,mid);
             return false;
        }
         collect(bv,mid);
        return true;
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
    public boolean checkVideo(long mid,String title){
        String sql = "select * from video v join post p on v.bv = p.bv where p.owner = ? and v.title = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,mid);
            stmt.setString(2,title);

            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            if(rs.next()){
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
    public boolean getVideo(String bv){
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
    public boolean getDanmu(String bv){
        String sql = "select * from danmu where bv = ?;";
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
    public long getOwner(String bv){
        String sql = "select * from post where bv = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);
            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            if(rs.next()){
                long l = rs.getLong(1);
                rs.close();
                stmt.close();
                conn.close();
                return l;
            }
            rs.close();
            stmt.close();
            conn.close();
            return 0;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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
    public boolean checkVideo(String bv,PostVideoReq postVideoReq){
        String sql = "SELECT duration,description,title,publicTime from video where bv = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)){
            stmt.setString(1, bv);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            if(rs.getLong(1)!=postVideoReq.getDuration()){
                rs.close();
                stmt.close();
                conn.close();
                return false;
            }
            if(postVideoReq.getDescription()!=null&&rs.getString(2).equals(postVideoReq.getDescription())&&rs.getString(3).equals(postVideoReq.getTitle()) && rs.getTimestamp(4).equals(postVideoReq.getPublicTime())){
                rs.close();
                stmt.close();
                conn.close();
                return false;
            }
            rs.close();
            stmt.close();
            conn.close();
            return true;
        }catch (SQLException e){
            throw new RuntimeException(e);
        }
    }
    public boolean duplicatedTitle(long mid,String bv,String title){
        String sql = "select p.bv from video v join post p on v.bv = p.bv where p.owner = ? and v.title = ? and v.bv != ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,mid);
            stmt.setString(2,title);
            stmt.setString(3,bv);


            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            if(rs.next()){
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
    public String getQuery(String keywords){
        String[] wordArr = keywords.split("\\s+");
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT a.bv, SUM(keyword_count) AS relevance,cnt FROM (");
        for (int i = 0; i < wordArr.length; i++) {
            if(i!= wordArr.length-1){
                sb.append("SELECT bv,  (LENGTH(title) - LENGTH(REPLACE(UPPER(title), UPPER(?), ''))) / LENGTH(UPPER(?)) AS keyword_count ");
                sb.append("FROM video ");
                sb.append("UNION ALL ");
                sb.append("SELECT bv,  (LENGTH(description) - LENGTH(REPLACE(UPPER(description), UPPER(?), ''))) / LENGTH(UPPER(?)) AS keyword_count ");
                sb.append("FROM video ");
                sb.append("UNION ALL ");
                sb.append("SELECT v.bv,  (LENGTH(name) - LENGTH(REPLACE(UPPER(name), UPPER(?), ''))) / LENGTH(UPPER(?)) AS keyword_count ");
                sb.append("FROM video v join post p on p.bv = v.bv join member m on m.mid = p.owner ");
                sb.append("UNION ALL ");
            }else{
                sb.append("SELECT bv,  (LENGTH(title) - LENGTH(REPLACE(UPPER(title), UPPER(?), ''))) / LENGTH(UPPER(?)) AS keyword_count ");
                sb.append("FROM video ");
                sb.append("UNION ALL ");
                sb.append("SELECT bv,  (LENGTH(description) - LENGTH(REPLACE(UPPER(description), UPPER(?), ''))) / LENGTH(UPPER(?)) AS keyword_count  ");
                sb.append("FROM video ");
                sb.append("UNION ALL ");
                sb.append("SELECT v.bv,  (LENGTH(name) - LENGTH(REPLACE(UPPER(name), UPPER(?), ''))) / LENGTH(UPPER(?)) AS keyword_count ");
                sb.append("FROM video v join post p on p.bv = v.bv join member m on m.mid = p.owner");
            }
        }
        sb.append(")a ");
        sb.append("join (select bv,count( mid) as cnt from viewvideo group by bv) b on a.bv = b.bv ");
        sb.append("group by a.bv,cnt having sum(keyword_count) > 0 ORDER BY relevance DESC, cnt DESC ");
        return sb.toString();
    }
    public String getQuery1(String keywords){
        String[] wordArr = keywords.split("\\s+");
        StringBuilder sb = new StringBuilder();

        sb.append("SELECT a.bv, SUM(keyword_count) AS relevance,cnt FROM (");
        for (int i = 0; i < wordArr.length; i++) {
            if(i!= wordArr.length-1){
                sb.append("SELECT v.bv,  (LENGTH(title) - LENGTH(REPLACE(UPPER(title), UPPER(?), ''))) / LENGTH(UPPER(?)) AS keyword_count ");
                sb.append("FROM video v join reviewVideo rv on v.bv = rv.bv join  post p on p.bv = v.bv where publicTime < CURRENT_TIMESTAMP and reviewTime < CURRENT_TIMESTAMP or p.owner = ? ");
                sb.append("UNION ALL ");
                sb.append("SELECT v.bv,  (LENGTH(description) - LENGTH(REPLACE(UPPER(description), UPPER(?), ''))) / LENGTH(UPPER(?)) AS keyword_count  ");
                sb.append("FROM video v join reviewVideo rv on v.bv = rv.bv join  post p on p.bv = v.bv where publicTime < CURRENT_TIMESTAMP and reviewTime < CURRENT_TIMESTAMP or p.owner = ? ");
                sb.append("UNION ALL ");
                sb.append("SELECT v.bv,  (LENGTH(name) - LENGTH(REPLACE(UPPER(name), UPPER(?), ''))) / LENGTH(UPPER(?)) AS keyword_count ");
                sb.append("FROM video v join post p on p.bv = v.bv join member m on m.mid = p.owner join reviewVideo rv on v.bv = rv.bv where publicTime < CURRENT_TIMESTAMP and reviewTime < CURRENT_TIMESTAMP or p.owner = ? ");
                sb.append("UNION ALL ");
            }else{
                sb.append("SELECT v.bv,  (LENGTH(title) - LENGTH(REPLACE(UPPER(title), UPPER(?), ''))) / LENGTH(UPPER(?)) AS keyword_count ");
                sb.append("FROM video v join reviewVideo rv on v.bv = rv.bv join  post p on p.bv = v.bv where publicTime < CURRENT_TIMESTAMP and reviewTime < CURRENT_TIMESTAMP or p.owner = ? ");
                sb.append("UNION ALL ");
                sb.append("SELECT v.bv,  (LENGTH(description) - LENGTH(REPLACE(UPPER(description), UPPER(?), ''))) / LENGTH(UPPER(?)) AS keyword_count ");
                sb.append("FROM video v join reviewVideo rv on v.bv = rv.bv join  post p on p.bv = v.bv where publicTime < CURRENT_TIMESTAMP and reviewTime < CURRENT_TIMESTAMP or p.owner = ? ");
                sb.append("UNION ALL ");
                sb.append("SELECT v.bv,  (LENGTH(name) - LENGTH(REPLACE(UPPER(name), UPPER(?), ''))) / LENGTH(UPPER(?)) AS keyword_count ");
                sb.append("FROM video v join post p on p.bv = v.bv join member m on m.mid = p.owner join reviewVideo rv on v.bv = rv.bv where publicTime < CURRENT_TIMESTAMP and reviewTime < CURRENT_TIMESTAMP or p.owner = ? ");
            }
        }
        sb.append(")a ");
        sb.append("join (select bv,count( mid) as cnt from viewvideo group by bv) b on a.bv = b.bv ");
        sb.append("group by a.bv,cnt having sum(keyword_count) > 0 ORDER BY relevance DESC, cnt DESC ");

        return sb.toString();
    }
    public boolean checkView(String bv){
        String sql = "select * from viewVideo where bv = ?;";
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
    public double getAVGViewTime(String bv){
        String sql = "select AVG(viewTime) from viewVideo where bv = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);


            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            rs.next();
            float f = rs.getFloat(1);
            rs.close();
            stmt.close();
            conn.close();
            return f;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean checkReview(String bv){
        String sql = "select * from reviewVideo where bv = ?;";
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
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean canSearch(long mid,String bv){
        String sql = "select publicTime,reviewMid from video v where bv = ? ;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);


            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            rs.next();
            if(rs.getTimestamp(1).after(Timestamp.valueOf(LocalDateTime.now()) )||rs.getLong(2)==0){
                if(!checkSuperUser(mid)&&getOwner(bv)!=mid){
                    rs.close();
                    stmt.close();
                    conn.close();
                    return false;
                }
            }
            rs.close();
            stmt.close();
            conn.close();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public long getCoin(long mid){
        String sql = "select coin from member where mid = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setLong(1,mid);
            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            rs.next();
            if(rs.getLong(1) <= 0){
                rs.close();
                stmt.close();
                conn.close();
                return -1;
            }
            long l = rs.getLong(1);
            rs.close();
            stmt.close();
            conn.close();
            return l;

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean hasCoin(String bv,long mid){
        String sql = "select * from coin where bv = ? and mid = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);
            stmt.setLong(2,mid);


            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            if(rs.next()){
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
    public boolean hasLike(String bv,long mid){
        String sql = "select * from likeVideo where bv = ? and mid = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);
            stmt.setLong(2,mid);


            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            if(rs.next()){
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
    public boolean like(String bv,long mid){
        String sql = "INSERT INTO likeVideo (bv,mid) values (?,?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);
            stmt.setLong(2,mid);

            stmt.executeUpdate();
            stmt.close();
            conn.close();

            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean unlike(String bv, long mid){
        String sql = "DELETE from likeVideo where bv = ? and mid = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);
            stmt.setLong(2,mid);

            stmt.executeUpdate();
            stmt.close();
            conn.close();
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean hasCollect(String bv,long mid){
        String sql = "select * from favorite where bv = ? and mid = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);
            stmt.setLong(2,mid);


            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();
            if(rs.next()){
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
    public boolean collect(String bv,long mid){
        String sql = "INSERT INTO favorite (bv,mid) values (?,?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);
            stmt.setLong(2,mid);

            stmt.executeUpdate();
            stmt.close();
            conn.close();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public boolean unCollect(String bv,long mid){
        String sql = "DELETE from favorite where bv = ? and mid = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);
            stmt.setLong(2,mid);

            stmt.executeUpdate();
            stmt.close();
            conn.close();
            return true;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public static String getRandomString(int length){
        String str="cvbnmlkjhgfdsaqwertyuiopQWERTYUIOPASDFGHJKLZXCVBNM1234567890";
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0; i<length; ++i){
            int number=random.nextInt(60);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
    public boolean hasReview(String bv){
        String sql = "select * from reviewvideo where bv = ?;";
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
    public void re_review(String bv){
        String sql = "delete from reviewvideo where bv = ?;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "update video set reviewmid = null where bv = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1,bv);

          stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
