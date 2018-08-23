package com.dylan.recom.webservice;

/**
 * Created by dylan
 */

import com.alibaba.fastjson.JSON;
import com.dylan.recom.common.ItemSimilarity;
import com.dylan.recom.common.RedisUtil;
import redis.clients.jedis.Jedis;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

@Path("/ws")
public class TestResult {
    Jedis jedis = null;

    public TestResult() {
        jedis = RedisUtil.getJedis();
    }

    @GET
    @Path("/{userid}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getRecoItems(@PathParam("userid") String userid) {

        // Stage 1: get user's items
        String value = jedis.get(userid);
        System.out.println(value);

        return value;
    }

}
