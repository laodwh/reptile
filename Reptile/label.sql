/*
Navicat MySQL Data Transfer

Source Server         : 192.166.169.81
Source Server Version : 50710
Source Host           : localhost:3306
Source Database       : label

Target Server Type    : MYSQL
Target Server Version : 50710
File Encoding         : 65001

Date: 2019-02-28 14:12:32
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for tb_label_keywords
-- ----------------------------
DROP TABLE IF EXISTS `tb_label_keywords`;
CREATE TABLE `tb_label_keywords` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `key_id` varchar(255) DEFAULT NULL,
  `key_text` varchar(255) DEFAULT NULL,
  `review_id` varchar(255) DEFAULT NULL,
  `movie_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=290 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for tb_label_movie
-- ----------------------------
DROP TABLE IF EXISTS `tb_label_movie`;
CREATE TABLE `tb_label_movie` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `year` varchar(255) DEFAULT NULL,
  `rate` varchar(100) DEFAULT NULL,
  `runtime` varchar(100) DEFAULT NULL,
  `summary` text CHARACTER SET utf8mb4,
  `directors` varchar(100) DEFAULT NULL,
  `stars` text,
  `category` varchar(255) DEFAULT NULL,
  `movie_id` varchar(100) DEFAULT NULL,
  `image_path` varchar(255) DEFAULT NULL,
  `plat_form` varchar(255) DEFAULT NULL,
  `review_url` varchar(255) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for tb_label_review
-- ----------------------------
DROP TABLE IF EXISTS `tb_label_review`;
CREATE TABLE `tb_label_review` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `review_id` varchar(255) DEFAULT NULL,
  `review_title` text CHARACTER SET utf8mb4,
  `review_text` text CHARACTER SET utf8mb4,
  `review_time` varchar(255) DEFAULT NULL,
  `plat_form` varchar(255) DEFAULT NULL,
  `movie_id` varchar(255) DEFAULT NULL,
  `create_time` varchar(255) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=55 DEFAULT CHARSET=utf8;
