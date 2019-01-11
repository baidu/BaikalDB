### How To Upgrade BaikalStore to the space-efficient-snapshot version

If your cluster is deployed with the binary built from the code submitted before Dec. 25 2018, you are required to follow the below instructions to upgrade the BaikalStore binary to the latest version.

1. Upgrade all BaikalStore instances to the latest no_snapshot_compatible branch. This is an intermediate branch, on which all add_peer operations will fail.
2. Create new snapshot manually for all regions.

 - How to create snapshot? Execute `sh src/tools/script/store_snapshot_region.sh ip:port` for all BaikalStore instances.
 - How to verify? Check the snapshot directory of each region. Each directory should only contain the file with *raft_snapshot_meta suffix, and no snap_region****.extra.json file.

3. Upgrade all BaikalStore instances to the latest master branch.



### baikalStore升级到新版本注意事项

如果线上运行版本是2018年12月25号(包括)以前提交的版本，若要升级到最新版，需要按照以下步骤进行滚动升级：

1. 先将线上所有baikalStore实例升级到no_snapshot_compatible分支的最新版本。该版本是一个中间版本，其中的add_peer操作会失败。
2. 将所有baikalStore实例中的region主动调用一次snapshot，生成新版snapshot。

 -   升级方法：为每个store实例执行sh store_snapshot_region.sh ip:port（store_snapshot_region.sh脚本位于src/tools/script/目录下）。
 -   是否升级到新版可以通过验证每个region的snapshot目录下是否还存在snap_region_***.extra.json文件。预期新版的snapshot目录下没有该文件，只有__raft_snapshot_meta后缀的文件。

3. 升级baikalStore到主干最新代码对应的程序。

**注意：由于使用了raft协议，BaikalStore与BaikalMeta的正常滚动升级或重启的并发度必须是1，否则过程中可能会影响服务的可用性。**
