package com.uzi.controller;

import com.uzi.entity.User;
import com.uzi.utils.EsUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class EsController {

    @Autowired
    private EsUtil<User> esUtil;

    @RequestMapping("/existsIndex/{indexName}")
    public void existsIndex(@PathVariable String indexName) throws Exception {
        System.out.println(esUtil.existsIndex(indexName));
    }

    @RequestMapping("/createIndex/{indexName}")
    public void createIndex(@PathVariable String indexName) throws Exception {
        System.out.println(esUtil.createIndex(indexName));
    }

    @RequestMapping("/deleteIndex/{indexName}")
    public void deleteIndex(@PathVariable String indexName) throws Exception {
        System.out.println(esUtil.deleteIndex(indexName));
    }

    @RequestMapping("/addDoc/{indexName}/{id}")
    public void addDoc(@PathVariable String indexName, @PathVariable String id) throws Exception {
        User user = new User();
        user.setName("uzi");
        user.setAge(10);
        esUtil.addDoc(indexName, id, user);
    }

    @RequestMapping("/bulkAdd/{indexName}")
    public void bulkAdd(@PathVariable String indexName) throws Exception {
        List<User> userList = new ArrayList<>();
        userList.add(new User("uzi1", 11));
        userList.add(new User("uzi2", 22));
        System.out.println(esUtil.bulkAdd(indexName, userList));
    }

    @RequestMapping("/getDoc/{indexName}/{id}")
    public void getDoc(@PathVariable String indexName, @PathVariable String id) throws Exception {
        esUtil.getDoc(indexName, id);
    }

    @RequestMapping("/search/{indexName}/{searchText}")
    public void search(@PathVariable String indexName, @PathVariable String searchText) throws Exception {
        System.out.println(esUtil.search(indexName, searchText));
    }


    @RequestMapping("/searchTemplate/{indexName}/{searchText}")
    public void searchTemplate(@PathVariable String indexName, @PathVariable String searchText) throws Exception {
        System.out.println(esUtil.searchTemplate(indexName, searchText));
    }
}
