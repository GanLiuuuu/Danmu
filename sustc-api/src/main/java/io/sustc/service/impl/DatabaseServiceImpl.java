package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import io.sustc.service.VideoService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.security.SecurityProperties;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        //TODO: replace this with your own student IDs in your group
        return Arrays.asList(12210729);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        String sql = "alter table post drop constraint post_owner_fkey;\n" +
                "alter table video drop constraint video_bv_fkey;\n" +
                "alter table danmu drop constraint danmu_bv_fkey;\n" +
                "alter table danmu drop constraint danmu_mid_fkey;\n" +
                "alter table favorite drop constraint favorite_bv_fkey;\n" +
                "alter table favorite drop constraint favorite_mid_fkey;\n" +
                "alter table follow drop constraint follow_followed_id_fkey;\n" +
                "alter table follow drop constraint follow_member_id_fkey;\n" +
                "alter table likeDanmu drop constraint likedanmu_id_fkey;\n" +
                "alter table likeDanmu drop constraint likedanmu_mid_fkey;\n" +
                "alter table likeVideo drop constraint likevideo_bv_fkey;\n" +
                "alter table likeVideo drop constraint likevideo_mid_fkey;\n" +
                "alter table coin drop constraint coin_bv_fkey;\n" +
                "alter table coin drop constraint coin_mid_fkey;\n" +
                "alter table reviewVideo drop constraint reviewvideo_bv_fkey;\n" +
                "alter table reviewVideo drop constraint reviewvideo_mid_fkey;\n" +
                "alter table viewVideo drop constraint viewvideo_bv_fkey;\n" +
                "alter table viewVideo drop constraint viewvideo_mid_fkey;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long t1 = System.currentTimeMillis();
        importUser1(userRecords);
        importVideo(videoRecords);
        log.info("before threadL:{}" ,System.currentTimeMillis() - t1);

        Thread thread8 = new Thread(()-> importV1(videoRecords));
        Thread thread9 = new Thread(()-> importV2(videoRecords));
        Thread thread10 = new Thread(()-> importV3(videoRecords));
        Thread thread11 = new Thread(()-> importV4(videoRecords));
        Thread thread12 = new Thread(()->importV5(videoRecords));
        Thread thread13 = new Thread(()->importV6(videoRecords));
        Thread thread14 = new Thread(() -> importDanmu(danmuRecords));
        Thread thread15 = new Thread(()->importV21(videoRecords));
        Thread thread16 = new Thread(()->importV11(videoRecords));

        thread8.start();
        thread9.start();
        thread10.start();
        thread11.start();
        thread12.start();
        thread13.start();

        thread15.start();
        thread16.start();
//        thread8.start();
//        thread9.start();
//        thread10.start();
//        thread11.start();
//        thread12.start();




        thread14.start();
        try {


//            thread8.join();
//            thread9.join();
//            thread10.join();
//            thread11.join();
//            thread12.join();
            thread8.join();
            thread9.join();
            thread10.join();
            thread11.join();
            thread13.join();
            thread12.join();

            thread14.join();

            thread15.join();
            thread16.join();


        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Thread thread4 = new Thread(() -> importUser2(userRecords));
        Thread thread5 = new Thread(() -> importUser3(userRecords));
        Thread thread6 = new Thread(() -> importUser4(userRecords));
        Thread thread7 = new Thread(() -> importUser5(userRecords));
        Thread thread17 = new Thread(()->importUser6(userRecords));
        Thread thread18 = new Thread(()->importUser7(userRecords));
        Thread thread19 = new Thread(()->importUser8(userRecords));
        Thread thread20 = new Thread(()->importUser9(userRecords));
        Thread thread21 = new Thread(()->importUser10(userRecords));
        thread4.start();
        thread5.start();
        thread6.start();
        thread7.start();
        thread17.start();
        thread18.start();
        thread19.start();
        thread20.start();
        thread21.start();
        try {
            thread4.join();
            thread5.join();
            thread6.join();
            thread7.join();
            thread17.join();
            thread18.join();
            thread19.join();
            thread20.join();
            thread21.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
         sql ="alter table post add constraint f1 FOREIGN KEY (owner) references member(mid) on delete cascade ;\n" +
                "alter table video add constraint f2 foreign key (bv) references post(bv) on delete cascade ;\n" +
                "alter table danmu add constraint f3 foreign key (bv) references video(bv) on delete cascade ;\n" +
                "alter table danmu add constraint f4 foreign key (mid) references member(mid) on delete cascade ;\n" +
                "alter table favorite add constraint f5 foreign key (mid) references member(mid) on delete cascade ;\n" +
                "alter table favorite add constraint f6 foreign key (bv) references video(bv) on delete cascade ;\n" +
                "alter table follow add constraint f7 foreign key (followed_id) references member(mid) on delete cascade ;\n" +
                "alter table follow add constraint f8 foreign key (member_id) references member(mid) on delete cascade ;\n" +
                "alter table likeDanmu add constraint f9 foreign key (id) references danmu(id) on delete cascade ;\n" +
                "alter table likeDanmu add constraint f10 foreign key (mid) references member(mid) on delete cascade ;\n" +
                "alter table likeVideo add constraint f11 foreign key (bv) references video(bv) on delete cascade ;\n" +
                "alter table likeVideo add constraint f12 foreign key (mid) references member(mid) on delete cascade ;\n" +
                "alter table coin add constraint f13 foreign key (bv) references video(bv) on delete cascade ;\n" +
                "alter table coin add constraint f14 foreign key (mid) references member(mid) on delete cascade ;\n" +
                "alter table reviewVideo add constraint f15 foreign key (bv) references video(bv) on delete cascade ;\n" +
                "alter table reviewVideo add constraint f16 foreign key (mid) references member(mid) on delete cascade ;\n" +
                "alter table viewVideo add constraint f17 foreign key (bv) references video(bv) on delete cascade ;\n" +
                "alter table viewVideo add constraint f18 foreign key (mid) references member(mid) on delete cascade ;";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        log.info("time:{} ", System.currentTimeMillis() - t1);
    }

    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            int i =  rs.getInt(1);
            rs.close();
            return i;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void importDanmu(List<DanmuRecord> danmuRecord){


        String sql = "insert into danmu(bv,mid,displayTime,content,postTime,id) values(?,?,?,?,?,?);";

        ArrayList<Long> ids = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (int i = 0; i < danmuRecord.size(); i++) {
                stmt.setString(1, danmuRecord.get(i).getBv());
                stmt.setLong(2, danmuRecord.get(i).getMid());
                stmt.setFloat(3,danmuRecord.get(i).getTime());
                stmt.setString(4,danmuRecord.get(i).getContent());
                stmt.setTimestamp(5,danmuRecord.get(i).getPostTime());
                long l = getRandomID(ids);
                ids.add(l);

                stmt.setLong(6,l);
                stmt.addBatch();
                if(i%500==0){
                    stmt.executeBatch();
                    conn.commit();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();
            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "insert into likeDanmu(id,mid) values (?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            int count = 0;
            for (int i = 0; i < danmuRecord.size(); i++) {
                for (int j = 0; j < danmuRecord.get(i).getLikedBy().length; j++) {
                    stmt.setLong(1,ids.get(i));
                    stmt.setLong(2,danmuRecord.get(i).getLikedBy()[j]);
                    stmt.addBatch();
                    count++;
                    if(count%500==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }

            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


    }
    public void importUser1(List<UserRecord> userRecord){
        long t = System.currentTimeMillis();
        String sql = "insert into member(mid,name,birthday,level,sign,sex,coin,qq,wechat,identity,password) values (?,?,?,?,?,?,?,?,?,?,?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (int i = 0; i < userRecord.size(); i++) {
                stmt.setLong(1, userRecord.get(i).getMid());
                stmt.setString(2, userRecord.get(i).getName());
                stmt.setString(3,userRecord.get(i).getBirthday());
                stmt.setInt(4,userRecord.get(i).getLevel());
                stmt.setString(5, userRecord.get(i).getSign());
                stmt.setString(6,userRecord.get(i).getSex());
                stmt.setInt(7,userRecord.get(i).getCoin());
                stmt.setString(8,userRecord.get(i).getQq());
                stmt.setString(9,userRecord.get(i).getWechat());
                if(userRecord.get(i).getIdentity() == UserRecord.Identity.USER){
                    stmt.setInt(10,0);
                }
                else {
                    stmt.setInt(10,1);
                }
                stmt.setString(11, userRecord.get(i).getPassword());
                stmt.addBatch();
                if(i%500==0){
                    stmt.executeBatch();
                    conn.commit();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();
            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();

        log.info("time1:{}",end - t);
    }

    public void importUser2(List<UserRecord> userRecord){
        long t = System.currentTimeMillis();
        String sql = "insert into follow(followed_id,member_id) values (?,?)";
        int count = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            for (int i = 0; i < userRecord.size()/9; i++) {
                for (int j = 0; j < userRecord.get(i).getFollowing().length; j++) {
                    stmt.setLong(1, userRecord.get(i).getFollowing()[j]);
                    stmt.setLong(2, userRecord.get(i).getMid());
                    stmt.addBatch();
                    count++;
                    if(count % 500 ==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long endtime = System.currentTimeMillis();
        log.info("time2:{}", endtime-t);
    }

    public void importUser3(List<UserRecord> userRecord){
        long t = System.currentTimeMillis();
        String sql = "insert into follow(followed_id,member_id) values (?,?)";
        int count = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            for (int i = userRecord.size()/9; i < 2*userRecord.size()/9; i++) {
                for (int j = 0; j < userRecord.get(i).getFollowing().length; j++) {
                    stmt.setLong(1, userRecord.get(i).getFollowing()[j]);
                    stmt.setLong(2, userRecord.get(i).getMid());
                    stmt.addBatch();
                    count++;
                    if(count % 500 ==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long endtime = System.currentTimeMillis();
        log.info("time2:{}", endtime-t);
    }
    public void importUser4(List<UserRecord> userRecord){
        long t = System.currentTimeMillis();
        String sql = "insert into follow(followed_id,member_id) values (?,?)";
        int count = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            for (int i = 2*userRecord.size()/9; i < 3*userRecord.size()/9; i++) {
                for (int j = 0; j < userRecord.get(i).getFollowing().length; j++) {
                    stmt.setLong(1, userRecord.get(i).getFollowing()[j]);
                    stmt.setLong(2, userRecord.get(i).getMid());
                    stmt.addBatch();
                    count++;
                    if(count % 500 ==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long endtime = System.currentTimeMillis();
        log.info("time2:{}", endtime-t);
    }
    public void importUser5(List<UserRecord> userRecord){
        long t = System.currentTimeMillis();
        String sql = "insert into follow(followed_id,member_id) values (?,?)";
        int count = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            for (int i = 3*userRecord.size()/9; i < 4*userRecord.size()/9; i++) {
                for (int j = 0; j < userRecord.get(i).getFollowing().length; j++) {
                    stmt.setLong(1, userRecord.get(i).getFollowing()[j]);
                    stmt.setLong(2, userRecord.get(i).getMid());
                    stmt.addBatch();
                    count++;
                    if(count % 500 ==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long endtime = System.currentTimeMillis();
        log.info("time2:{}", endtime-t);
    }
    public void importUser6(List<UserRecord> userRecord){
        long t = System.currentTimeMillis();
        String sql = "insert into follow(followed_id,member_id) values (?,?)";
        int count = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            for (int i = 4*userRecord.size()/9; i < 5*userRecord.size()/9; i++) {
                for (int j = 0; j < userRecord.get(i).getFollowing().length; j++) {
                    stmt.setLong(1, userRecord.get(i).getFollowing()[j]);
                    stmt.setLong(2, userRecord.get(i).getMid());
                    stmt.addBatch();
                    count++;
                    if(count % 500 ==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long endtime = System.currentTimeMillis();
        log.info("time2:{}", endtime-t);
    }
    public void importUser7(List<UserRecord> userRecord){
        long t = System.currentTimeMillis();
        String sql = "insert into follow(followed_id,member_id) values (?,?)";
        int count = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            for (int i =5* userRecord.size()/9; i < 6*userRecord.size()/9; i++) {
                for (int j = 0; j < userRecord.get(i).getFollowing().length; j++) {
                    stmt.setLong(1, userRecord.get(i).getFollowing()[j]);
                    stmt.setLong(2, userRecord.get(i).getMid());
                    stmt.addBatch();
                    count++;
                    if(count % 500 ==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long endtime = System.currentTimeMillis();
        log.info("time2:{}", endtime-t);
    }
    public void importUser8(List<UserRecord> userRecord){
        long t = System.currentTimeMillis();
        String sql = "insert into follow(followed_id,member_id) values (?,?)";
        int count = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            for (int i = 6*userRecord.size()/9; i < 7*userRecord.size()/9; i++) {
                for (int j = 0; j < userRecord.get(i).getFollowing().length; j++) {
                    stmt.setLong(1, userRecord.get(i).getFollowing()[j]);
                    stmt.setLong(2, userRecord.get(i).getMid());
                    stmt.addBatch();
                    count++;
                    if(count % 500 ==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long endtime = System.currentTimeMillis();
        log.info("time2:{}", endtime-t);
    }
    public void importUser9(List<UserRecord> userRecord){
        long t = System.currentTimeMillis();
        String sql = "insert into follow(followed_id,member_id) values (?,?)";
        int count = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            for (int i = 7*userRecord.size()/9; i < 8*userRecord.size()/9; i++) {
                for (int j = 0; j < userRecord.get(i).getFollowing().length; j++) {
                    stmt.setLong(1, userRecord.get(i).getFollowing()[j]);
                    stmt.setLong(2, userRecord.get(i).getMid());
                    stmt.addBatch();
                    count++;
                    if(count % 500 ==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long endtime = System.currentTimeMillis();
        log.info("time2:{}", endtime-t);
    }
    public void importUser10(List<UserRecord> userRecord){
        long t = System.currentTimeMillis();
        String sql = "insert into follow(followed_id,member_id) values (?,?)";
        int count = 0;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            for (int i = 8*userRecord.size()/9; i < userRecord.size(); i++) {
                for (int j = 0; j < userRecord.get(i).getFollowing().length; j++) {
                    stmt.setLong(1, userRecord.get(i).getFollowing()[j]);
                    stmt.setLong(2, userRecord.get(i).getMid());
                    stmt.addBatch();
                    count++;
                    if(count % 500 ==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        long endtime = System.currentTimeMillis();
        log.info("time2:{}", endtime-t);
    }
    public void importVideo(List<VideoRecord> videoRecord){
        long start = System.currentTimeMillis();
        String sql = "insert into post(owner,bv) values (?,?);";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)
        ) {
            conn.setAutoCommit(false);
            for (int i = 0; i < videoRecord.size(); i++) {
                stmt.setLong(1,videoRecord.get(i).getOwnerMid());
                stmt.setString(2,videoRecord.get(i).getBv());
                stmt.addBatch();
                if(i%500==0){
                    stmt.executeBatch();
                    conn.commit();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        sql = "insert into video(bv,title,description,publicTime,commitTime,duration,reviewMid) values (?,?,?,?,?,?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)
        ) {
            conn.setAutoCommit(false);
            for (int i = 0; i < videoRecord.size(); i++) {

                stmt.setString(1, videoRecord.get(i).getBv());
                stmt.setString(2, videoRecord.get(i).getTitle());
                stmt.setString(3,videoRecord.get(i).getDescription());
                stmt.setTimestamp(4,videoRecord.get(i).getPublicTime());
                stmt.setTimestamp(5,videoRecord.get(i).getCommitTime());
                stmt.setFloat(6, videoRecord.get(i).getDuration());
                stmt.setLong(7,videoRecord.get(i).getReviewer());
                stmt.addBatch();
                if(i%500==0){
                    stmt.executeBatch();
                    conn.commit();
                    stmt.clearBatch();
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }








        sql = "insert into reviewVideo(mid,bv,reviewTime) values (?,?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);
            int count = 0;
            for (int i = 0; i < videoRecord.size(); i++) {
                stmt.setLong(1, videoRecord.get(i).getReviewer());
                stmt.setString(2, videoRecord.get(i).getBv());
                stmt.setTimestamp(3,videoRecord.get(i).getReviewTime());
                stmt.addBatch();
                count++;
                if(count%500==0){
                    stmt.executeBatch();
                    conn.commit();
                    stmt.clearBatch();
                    count = 0;
                }
            }

            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();
            conn.setAutoCommit(true);



        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        long endtime = System.currentTimeMillis();

    }
    public void importV1(List<VideoRecord> videoRecord){
        long t = System.currentTimeMillis();
        String sql = "insert into viewVideo(mid,bv,viewTime) values (?,?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);
            int count = 0;
            for (int i = 0; i < videoRecord.size()/4; i++) {
                for (int j = 0; j < videoRecord.get(i).getViewerMids().length; j++) {
                    stmt.setLong(1, videoRecord.get(i).getViewerMids()[j]);
                    stmt.setString(2, videoRecord.get(i).getBv());
                    stmt.setFloat(3,videoRecord.get(i).getViewTime()[j]);
                    stmt.addBatch();
                    count++;
                    if(count%500==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);

        log.info("time of vv:{}" ,System.currentTimeMillis() - t);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void importV2(List<VideoRecord> videoRecord){
        String sql = "insert into viewVideo(mid,bv,viewTime) values (?,?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);
            int count = 0;
            for (int i = videoRecord.size()/4; i < 2*videoRecord.size()/4; i++) {
                for (int j = 0; j < videoRecord.get(i).getViewerMids().length; j++) {
                    stmt.setLong(1, videoRecord.get(i).getViewerMids()[j]);
                    stmt.setString(2, videoRecord.get(i).getBv());
                    stmt.setFloat(3,videoRecord.get(i).getViewTime()[j]);
                    stmt.addBatch();
                    count++;
                    if(count%500==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void importV11(List<VideoRecord> videoRecord){
        String sql = "insert into viewVideo(mid,bv,viewTime) values (?,?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);
            int count = 0;
            for (int i = 2*videoRecord.size()/4; i < 3*videoRecord.size()/4; i++) {
                for (int j = 0; j < videoRecord.get(i).getViewerMids().length; j++) {
                    stmt.setLong(1, videoRecord.get(i).getViewerMids()[j]);
                    stmt.setString(2, videoRecord.get(i).getBv());
                    stmt.setFloat(3,videoRecord.get(i).getViewTime()[j]);
                    stmt.addBatch();
                    count++;
                    if(count%500==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void importV21(List<VideoRecord> videoRecord){
        String sql = "insert into viewVideo(mid,bv,viewTime) values (?,?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);
            int count = 0;
            for (int i = 3*videoRecord.size()/4; i < videoRecord.size(); i++) {
                for (int j = 0; j < videoRecord.get(i).getViewerMids().length; j++) {
                    stmt.setLong(1, videoRecord.get(i).getViewerMids()[j]);
                    stmt.setString(2, videoRecord.get(i).getBv());
                    stmt.setFloat(3,videoRecord.get(i).getViewTime()[j]);
                    stmt.addBatch();
                    count++;
                    if(count%500==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void importV3(List<VideoRecord> videoRecord){
        String sql = "insert into coin(mid,bv) values (?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);
            int count = 0;
            for (int i = 0; i < videoRecord.size()/2; i++) {
                for (int j = 0; j < videoRecord.get(i).getCoin().length; j++) {
                    stmt.setLong(1, videoRecord.get(i).getCoin()[j]);
                    stmt.setString(2, videoRecord.get(i).getBv());
                    stmt.addBatch();
                    count++;
                    if(count%500==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void importV4(List<VideoRecord> videoRecord){
        String sql = "insert into coin(mid,bv) values (?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);
            int count = 0;
            for (int i = videoRecord.size()/2; i < videoRecord.size(); i++) {
                for (int j = 0; j < videoRecord.get(i).getCoin().length; j++) {
                    stmt.setLong(1, videoRecord.get(i).getCoin()[j]);
                    stmt.setString(2, videoRecord.get(i).getBv());
                    stmt.addBatch();
                    count++;
                    if(count%500==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void importV5(List<VideoRecord> videoRecord){
        String sql = "insert into favorite(mid,bv) values (?,?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            int count = 0;
            for (int i = 0; i < videoRecord.size(); i++) {
                for (int j = 0; j < videoRecord.get(i).getFavorite().length; j++) {
                    stmt.setLong(1, videoRecord.get(i).getFavorite()[j]);
                    stmt.setString(2, videoRecord.get(i).getBv());
                    stmt.addBatch();
                    count++;
                    if(count%500==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    public void importV6(List<VideoRecord> videoRecord){
        String sql = "insert into likeVideo(mid,bv) values (?,?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);

            int count = 0;
            for (int i = 0; i < videoRecord.size(); i++) {
                for (int j = 0; j < videoRecord.get(i).getLike().length; j++) {
                    stmt.setLong(1, videoRecord.get(i).getLike()[j]);
                    stmt.setString(2, videoRecord.get(i).getBv());
                    stmt.addBatch();
                    count++;
                    if(count%500==0){
                        stmt.executeBatch();
                        conn.commit();
                        stmt.clearBatch();
                        count = 0;
                    }
                }
            }
            stmt.executeBatch();
            conn.commit();
            stmt.clearBatch();

            conn.setAutoCommit(true);




        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    public long getRandomID(ArrayList<Long> ids){
        if(!ids.isEmpty()){
            return Collections.max(ids) + 1;
        }
        else{
            return 1;
        }
    }
}