def twoSum(nums, target):
    num_dict = {k: 1 for k in nums}
    for i in xrange(len(nums)):
        if target-nums[i] in num_dict and nums.index(target-nums[i]) != i:
            fst_index = i+1
            sec_index = nums.index(target - nums[i]) + 1
            return [min(fst_index,sec_index),max(fst_index,sec_index)]