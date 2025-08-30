// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract RadarBriefingRegistry {
    event BriefingPublished(
        string day,
        string chain,
        bytes32 summaryHash,
        bool hasAnomaly,
        uint256 evidenceRows,
        string model,
        string uri
    );
    mapping(bytes32 => bytes32) public reports;

    function publish(
        string calldata day,
        string calldata chain,
        bytes32 summaryHash,
        bool hasAnomaly,
        uint256 evidenceRows,
        string calldata model,
        string calldata uri
    ) external {
        bytes32 key = keccak256(abi.encodePacked(day, "|", chain));
        reports[key] = summaryHash;
        emit BriefingPublished(day, chain, summaryHash, hasAnomaly, evidenceRows, model, uri);
    }

    function get(string calldata day, string calldata chain) external view returns (bytes32) {
        bytes32 key = keccak256(abi.encodePacked(day, "|", chain));
        return reports[key];
    }
}
