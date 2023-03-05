import { atom } from "recoil";

const isDebugModeState = atom({
    key: 'isDebugMode',
    default: false
});

export default isDebugModeState;
